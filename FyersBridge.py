#!/usr/bin/env python3
"""
FyersBridge.py
==============

FYERS-specific market-data process for the AmiBroker plug-in.

The user's existing atok.py remains responsible for browser login and for
creating the daily access_token.txt. This bridge never opens a browser and
never exchanges an authorization code.

Runtime path:
    access_token.txt
          |
          v
    official fyers-apiv3
          |
          +---- WebSocket SymbolUpdate (live)
          |
          +---- REST /history (5S + D)
          |
          v
    5-second engine / recovery
          |
          v
    localhost TCP 127.0.0.1:47321
          |
          v
    OpenAlgo.dll -> AmiBroker

Only market-data APIs are used. No order/trading API is imported or called.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import queue
import socket
import sys
import threading
import time
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from typing import Any, Optional

from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws

PROTOCOL_VERSION = 1
FIVE_SEC = 5
SECONDS_HISTORY_LOOKBACK_DAYS = 42
DAILY_LOOKBACK_DAYS = 730
DAILY_CHUNK_DAYS = 366
MAX_GAP_FILL_SEC = 3600

IPC_RECONNECT_SEC = 1.0
FYERS_RETRY_SEC = 3.0
TOKEN_POLL_SEC = 2.0
HISTORY_WORKERS = 2
HISTORY_BATCH_ROWS = 200

log = logging.getLogger("FyersBridge")


def setup_logging(base_dir: str) -> None:
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )

    if log.handlers:
        return

    log.setLevel(logging.INFO)

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    log.addHandler(stream)

    log_file = os.path.join(base_dir, "fyers_bridge.log")
    rotating = RotatingFileHandler(
        log_file,
        maxBytes=2_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    rotating.setFormatter(formatter)
    log.addHandler(rotating)


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def sanitize_message(value: Any, limit: int = 500) -> str:
    return str(value).replace("\t", " ").replace("\r", " ").replace("\n", " ")[:limit]


def token_expired(token: str) -> bool:
    """
    Best-effort local JWT expiration check.

    Failure to decode does not reject a token; FYERS remains authoritative.
    """
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return False

        encoded = parts[1] + "=" * (-len(parts[1]) % 4)
        payload = base64.urlsafe_b64decode(encoded.encode("ascii")).decode("utf-8")
        exp = int(json.loads(payload).get("exp", 0))
        return exp > 0 and exp <= int(time.time())
    except Exception:
        return False


def read_token(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        token = handle.read().strip()

    if not token:
        raise RuntimeError(f"access_token.txt is empty: {path}")

    return token


def is_index_symbol(symbol: str) -> bool:
    return symbol.upper().endswith("-INDEX")


def extract_timestamp(tick: dict[str, Any], index: bool) -> int:
    keys = (
        ("exch_feed_time", "last_traded_time", "tt")
        if index
        else ("last_traded_time", "exch_feed_time", "tt")
    )

    for key in keys:
        ts = safe_int(tick.get(key), 0)
        if ts > 0:
            return ts

    return 0


@dataclass
class FiveSecBar:
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    oi: float = 0.0


class SymbolState:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.current: Optional[FiveSecBar] = None
        self.last_completed_ts = 0
        self.last_seen_ts = 0

        # FYERS provides cumulative traded volume for tradable symbols.
        # We turn cumulative volume into per-5-second volume deltas.
        self.prev_total_volume: Optional[float] = None
        self.volume_day: Optional[str] = None

        self.lock = threading.Lock()


@dataclass(frozen=True)
class HistoryTask:
    symbol: str
    resolution: str
    start_ts: int
    end_ts: int
    reason: str


class FyersBridge:
    def __init__(self, args: argparse.Namespace) -> None:
        self.app_id = args.app_id.strip()
        self.token_path = os.path.abspath(args.token_file)
        self.host = args.host
        self.port = args.port

        self.stop_event = threading.Event()

        self.symbols: set[str] = set()
        self.symbol_lock = threading.Lock()

        self.states: dict[str, SymbolState] = {}
        self.states_lock = threading.Lock()

        self.fyers_rest: Optional[fyersModel.FyersModel] = None
        self.rest_lock = threading.Lock()

        self.token_value = ""
        self.token_mtime = 0.0

        self.ipc_socket: Optional[socket.socket] = None
        self.ipc_lock = threading.Lock()
        self.ipc_connected = threading.Event()

        self.ws: Optional[data_ws.FyersDataSocket] = None
        self.ws_lock = threading.Lock()
        self.ws_connected = threading.Event()

        self.history_queue: queue.Queue[HistoryTask] = queue.Queue()
        self.history_pending: set[tuple[str, str, int, int]] = set()
        self.history_pending_lock = threading.Lock()
        self.history_threads: list[threading.Thread] = []

        self.ipc_thread = threading.Thread(
            target=self.ipc_server_loop,
            name="FyersBridge-IPC",
            daemon=True,
        )
        self.ws_thread = threading.Thread(
            target=self.websocket_supervisor,
            name="FyersBridge-WS",
            daemon=True,
        )

    # ------------------------------------------------------------------
    # Local IPC
    # ------------------------------------------------------------------
    def send_line(self, line: str) -> bool:
        if not line:
            return False

        payload = (line + "\n").encode("utf-8", errors="replace")

        with self.ipc_lock:
            sock = self.ipc_socket
            if sock is None:
                return False

            try:
                sock.sendall(payload)
                return True
            except OSError as exc:
                log.debug("IPC send failed: %s", exc)
                return False

    def ipc_server_loop(self) -> None:
        """
        Python owns the local listening endpoint.

        The DLL is a reconnecting TCP client. Only loopback (127.0.0.1) is
        used, so market data never leaves the local machine through this IPC.
        """
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            server.bind((self.host, self.port))
            server.listen(1)
            server.settimeout(1.0)
            log.info("Local IPC server listening on %s:%d", self.host, self.port)

            while not self.stop_event.is_set():
                try:
                    sock, address = server.accept()
                except socket.timeout:
                    continue
                except OSError as exc:
                    if not self.stop_event.is_set():
                        log.warning("IPC accept failed: %s", exc)
                    continue

                log.info("OpenAlgo.dll connected from %s:%d", address[0], address[1])

                with self.ipc_lock:
                    old = self.ipc_socket
                    self.ipc_socket = sock

                if old is not None:
                    try:
                        old.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    try:
                        old.close()
                    except OSError:
                        pass

                self.ipc_connected.set()
                self.send_line(
                    f"READY\\t{PROTOCOL_VERSION}\\t5S\\tofficial-python-sdk"
                )

                pending = b""

                try:
                    sock.settimeout(2.0)

                    while not self.stop_event.is_set():
                        try:
                            chunk = sock.recv(65536)
                        except socket.timeout:
                            self.send_line(f"PONG\\t{int(time.time())}")
                            continue

                        if not chunk:
                            break

                        pending += chunk

                        while b"\\n" in pending:
                            raw, pending = pending.split(b"\\n", 1)
                            line = raw.decode(
                                "utf-8",
                                errors="replace",
                            ).strip()

                            if line:
                                self.handle_command(line)

                except OSError as exc:
                    log.debug("IPC receive failed: %s", exc)

                finally:
                    self.ipc_connected.clear()

                    with self.ipc_lock:
                        if self.ipc_socket is sock:
                            self.ipc_socket = None

                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

                    try:
                        sock.close()
                    except OSError:
                        pass

                if not self.stop_event.is_set():
                    log.warning(
                        "OpenAlgo.dll IPC disconnected; waiting for reconnect"
                    )

        finally:
            try:
                server.close()
            except OSError:
                pass

    def handle_command(self, line: str) -> None:
        parts = line.split("\\t")
        command = parts[0].upper()

        if command == "HELLO":
            self.send_line(
                f"READY\\t{PROTOCOL_VERSION}\\t5S\\tofficial-python-sdk"
            )
            return

        if command == "SUB" and len(parts) >= 2:
            symbol = parts[1].strip()

            if symbol:
                with self.symbol_lock:
                    self.symbols.add(symbol)

                self.ensure_state(symbol)
                self.subscribe_symbols([symbol])

            return

        if command == "SYNC" and len(parts) >= 3:
            symbol = parts[1].strip()
            last_ts = safe_int(parts[2], 0)

            if symbol:
                self.ensure_state(symbol)
                self.queue_sync(symbol, last_ts)

            return

        if command == "TESTREST":
            self.test_rest()
            return

        if command == "TESTWS":
            if self.ws_connected.wait(timeout=12.0):
                self.send_line(
                    "TESTWS_OK\\tOfficial FYERS Python DataSocket is connected."
                )
            else:
                self.send_line(
                    "TESTWS_ERR\\tOfficial FYERS Python DataSocket did not become connected within 12 seconds."
                )
            return

        if command == "PING":
            self.send_line(f"PONG\\t{int(time.time())}")
            return

        if command == "SHUTDOWN":
            log.info("Shutdown requested by AmiBroker DLL")
            self.stop_event.set()
            self.disconnect_ws()
            return
    # ------------------------------------------------------------------
    # FYERS authentication/client objects
    # ------------------------------------------------------------------
    def refresh_clients_from_token_file(self) -> bool:
        try:
            stat = os.stat(self.token_path)
        except OSError as exc:
            self.send_line(f"AUTH_EXPIRED\taccess_token.txt unavailable: {sanitize_message(exc)}")
            return False

        mtime = stat.st_mtime

        if mtime == self.token_mtime and self.token_value:
            return True

        token = read_token(self.token_path)

        if token_expired(token):
            self.token_value = ""
            self.token_mtime = mtime
            self.send_line(
                "AUTH_EXPIRED\tThe daily access token in access_token.txt is expired."
            )
            return False

        self.token_value = token
        self.token_mtime = mtime

        with self.rest_lock:
            self.fyers_rest = fyersModel.FyersModel(
                client_id=self.app_id,
                token=token,
                is_async=False,
                log_path="",
            )

        log.info("Loaded FYERS daily access token from %s", self.token_path)
        self.send_line("STATUS\tTOKEN_READY\tDaily access token loaded from access_token.txt")
        return True

    # ------------------------------------------------------------------
    # FYERS WebSocket
    # ------------------------------------------------------------------
    def websocket_supervisor(self) -> None:
        while not self.stop_event.is_set():
            try:
                if not self.refresh_clients_from_token_file():
                    time.sleep(TOKEN_POLL_SEC)
                    continue

                ws_auth = f"{self.app_id}:{self.token_value}"

                ws = data_ws.FyersDataSocket(
                    access_token=ws_auth,
                    log_path="",
                    litemode=False,
                    write_to_file=False,
                    reconnect=True,
                    on_connect=self.on_connect,
                    on_close=self.on_close,
                    on_error=self.on_error,
                    on_message=self.on_message,
                )

                # FYERS exposes a queue-processing interval. A low but
                # non-zero value keeps latency responsive without busy-looping.
                try:
                    setter = getattr(ws, "setQueueProcessInterval", None)
                    if callable(setter):
                        setter(50)
                except Exception:
                    pass

                with self.ws_lock:
                    self.ws = ws

                log.info("Starting official FYERS market-data WebSocket")
                self.send_line(
                    "STATUS\tCONNECTING\tOfficial FYERS Python DataSocket starting"
                )

                ws.connect()
                ws.keep_running()

            except Exception as exc:
                msg = sanitize_message(exc)
                log.warning("FYERS WebSocket cycle failed: %s", msg)
                self.send_line(f"STATUS\tDISCONNECTED\t{msg}")

                if self.looks_like_auth_error(msg):
                    self.send_line(f"AUTH_EXPIRED\t{msg}")

            finally:
                self.ws_connected.clear()

                with self.ws_lock:
                    self.ws = None

            if not self.stop_event.is_set():
                time.sleep(FYERS_RETRY_SEC)

    @staticmethod
    def looks_like_auth_error(message: str) -> bool:
        text = message.lower()
        return any(
            marker in text
            for marker in (
                "unauthorized",
                "invalid token",
                "token expired",
                "401",
                "-8",
                "-15",
                "-16",
                "-17",
            )
        )

    def on_connect(self) -> None:
        self.ws_connected.set()
        log.info("FYERS WebSocket connected/authenticated")
        self.send_line(
            "STATUS\tCONNECTED\tFYERS Python DataSocket authenticated"
        )

        with self.symbol_lock:
            symbols = sorted(self.symbols)

        self.subscribe_symbols(symbols)

    def subscribe_symbols(self, symbols: list[str]) -> None:
        if not symbols:
            return

        with self.ws_lock:
            ws = self.ws

        if ws is None:
            return

        try:
            ws.subscribe(
                symbols=symbols,
                data_type="SymbolUpdate",
                channel=15,
            )
            log.info("FYERS subscription refreshed for %d symbol(s)", len(symbols))
        except Exception as exc:
            msg = sanitize_message(exc)
            log.warning("FYERS subscribe failed: %s", msg)
            self.send_line(f"ERROR\tSUBSCRIBE\t{msg}")

    def disconnect_ws(self) -> None:
        with self.ws_lock:
            ws = self.ws

        if ws is not None:
            try:
                ws.disconnect()
            except Exception:
                pass

    def on_close(self, message: Any) -> None:
        self.ws_connected.clear()
        msg = sanitize_message(message, 300)
        log.warning("FYERS WebSocket closed: %s", msg)
        self.send_line(
            f"STATUS\tDISCONNECTED\t{msg or 'socket closed; SDK reconnect is enabled'}"
        )

    def on_error(self, message: Any) -> None:
        msg = sanitize_message(message)
        log.warning("FYERS WebSocket error: %s", msg)
        self.send_line(f"STATUS\tERROR\t{msg}")

        if self.looks_like_auth_error(msg):
            self.send_line(f"AUTH_EXPIRED\t{msg}")

    # ------------------------------------------------------------------
    # Live data / 5-second aggregation
    # ------------------------------------------------------------------
    def flatten_message(self, message: Any) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []

        def walk(value: Any) -> None:
            if isinstance(value, list):
                for item in value:
                    walk(item)
                return

            if isinstance(value, dict):
                if "d" in value:
                    walk(value["d"])
                    return

                if "val" in value and isinstance(value["val"], (list, dict)):
                    walk(value["val"])
                    return

                result.append(value)

        walk(message)
        return result

    def on_message(self, message: Any) -> None:
        for tick in self.flatten_message(message):
            try:
                self.process_tick(tick)
            except Exception:
                log.exception("Unhandled error while processing FYERS update")

    def ensure_state(self, symbol: str) -> SymbolState:
        with self.states_lock:
            state = self.states.get(symbol)

            if state is None:
                state = SymbolState(symbol)
                self.states[symbol] = state

            return state

    def process_tick(self, tick: dict[str, Any]) -> None:
        symbol = str(tick.get("symbol") or "").strip()
        if not symbol:
            return

        msg_type = str(tick.get("type") or "").lower()
        if msg_type in {"cn", "sub", "unsub", "ful", "dp"}:
            return

        ltp = safe_float(tick.get("ltp"), 0.0)
        if ltp <= 0:
            return

        index = is_index_symbol(symbol)
        ts = extract_timestamp(tick, index)

        if ts <= 0:
            ts = int(time.time())

        state = self.ensure_state(symbol)

        with state.lock:
            self.emit_quote(state, tick, ts, ltp, index)
            self.update_5s_bar(state, tick, ts, ltp, index)

    def emit_quote(
        self,
        state: SymbolState,
        tick: dict[str, Any],
        ts: int,
        ltp: float,
        index: bool,
    ) -> None:
        total_volume = 0.0 if index else safe_float(tick.get("vol_traded_today"), 0.0)
        oi = safe_float(
            tick.get(
                "oi",
                tick.get(
                    "open_interest",
                    tick.get("open_interest_quantity"),
                ),
            ),
            0.0,
        )

        prev = safe_float(tick.get("prev_close_price"), 0.0)
        open_price = safe_float(tick.get("open_price"), 0.0)
        high = safe_float(tick.get("high_price"), 0.0)
        low = safe_float(tick.get("low_price"), 0.0)
        bid = safe_float(tick.get("bid_price"), 0.0)
        ask = safe_float(tick.get("ask_price"), 0.0)
        last_qty = safe_float(tick.get("last_traded_qty"), 0.0)

        state.last_seen_ts = max(state.last_seen_ts, ts)

        self.send_line(
            "QUOTE\t{}\t{}\t{:.10g}\t{:.10g}\t{:.10g}\t{:.10g}\t{:.10g}\t{:.10g}\t{:.10g}\t{:.10g}\t{:.10g}\t{:.10g}".format(
                state.symbol,
                ts,
                ltp,
                prev,
                open_price,
                high,
                low,
                total_volume,
                oi,
                bid,
                ask,
                last_qty,
            )
        )

    def volume_delta(
        self,
        state: SymbolState,
        tick: dict[str, Any],
        ts: int,
        index: bool,
    ) -> float:
        if index:
            return 0.0

        total = tick.get("vol_traded_today")
        qty = safe_float(tick.get("last_traded_qty"), 0.0)
        day = time.strftime("%Y-%m-%d", time.localtime(ts))

        if state.volume_day != day:
            state.volume_day = day
            state.prev_total_volume = safe_float(total, qty)
            return max(0.0, qty)

        if total is None:
            return max(0.0, qty)

        current = safe_float(total, 0.0)

        if state.prev_total_volume is None:
            delta = max(0.0, qty)
        elif current >= state.prev_total_volume:
            delta = current - state.prev_total_volume
        else:
            # Session reset/correction.
            delta = max(0.0, qty)

        state.prev_total_volume = current
        return delta

    def update_5s_bar(
        self,
        state: SymbolState,
        tick: dict[str, Any],
        ts: int,
        ltp: float,
        index: bool,
    ) -> None:
        bucket = (ts // FIVE_SEC) * FIVE_SEC
        delta_volume = self.volume_delta(state, tick, ts, index)
        oi = safe_float(tick.get("oi"), 0.0)

        if state.current is None:
            state.current = FiveSecBar(
                ts=bucket,
                open=ltp,
                high=ltp,
                low=ltp,
                close=ltp,
                volume=delta_volume,
                oi=oi,
            )
            self.emit_bar(state.symbol, state.current, False)
            return

        if bucket < state.current.ts:
            # Delayed/out-of-order update. Do not corrupt a newer bar.
            return

        if bucket > state.current.ts:
            completed = state.current
            self.emit_bar(state.symbol, completed, True)
            state.last_completed_ts = completed.ts

            gap_start = completed.ts + FIVE_SEC
            gap_end = bucket - FIVE_SEC

            if gap_end >= gap_start:
                gap_seconds = gap_end - gap_start + FIVE_SEC

                if gap_seconds <= MAX_GAP_FILL_SEC:
                    self.queue_history(
                        HistoryTask(
                            symbol=state.symbol,
                            resolution="5S",
                            start_ts=gap_start,
                            end_ts=gap_end,
                            reason="live-gap",
                        )
                    )
                    self.send_line(
                        "STATUS\tGAP\t{} {}->{} queued for REST 5S repair".format(
                            state.symbol,
                            gap_start,
                            gap_end,
                        )
                    )
                else:
                    self.send_line(
                        "ERROR\tGAP\t{} gap of {} seconds exceeds {} second safety cap".format(
                            state.symbol,
                            gap_seconds,
                            MAX_GAP_FILL_SEC,
                        )
                    )

            state.current = FiveSecBar(
                ts=bucket,
                open=ltp,
                high=ltp,
                low=ltp,
                close=ltp,
                volume=delta_volume,
                oi=oi,
            )
        else:
            state.current.high = max(state.current.high, ltp)
            state.current.low = min(state.current.low, ltp)
            state.current.close = ltp
            state.current.volume += delta_volume
            if oi > 0:
                state.current.oi = oi

        self.emit_bar(state.symbol, state.current, False)

    def emit_bar(self, symbol: str, bar: FiveSecBar, final: bool) -> None:
        self.send_line(
            "BAR\t{}\t{}\t{:.10g}\t{:.10g}\t{:.10g}\t{:.10g}\t{:.10g}\t{:.10g}\t{}".format(
                symbol,
                bar.ts,
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.oi,
                1 if final else 0,
            )
        )

    # ------------------------------------------------------------------
    # Historical data
    # ------------------------------------------------------------------
    def queue_sync(self, symbol: str, last_ts: int) -> None:
        now = int(time.time())
        closed_now = (now // FIVE_SEC) * FIVE_SEC - FIVE_SEC

        if last_ts > 0:
            start = last_ts + FIVE_SEC

            if start <= closed_now:
                self.queue_history(
                    HistoryTask(
                        symbol=symbol,
                        resolution="5S",
                        start_ts=start,
                        end_ts=closed_now,
                        reason="dll-resync",
                    )
                )
            return

        self.queue_history(
            HistoryTask(
                symbol=symbol,
                resolution="5S",
                start_ts=max(
                    1,
                    closed_now - SECONDS_HISTORY_LOOKBACK_DAYS * 86400,
                ),
                end_ts=closed_now,
                reason="initial-5S",
            )
        )

        self.queue_history(
            HistoryTask(
                symbol=symbol,
                resolution="D",
                start_ts=max(
                    1,
                    now - DAILY_LOOKBACK_DAYS * 86400,
                ),
                end_ts=now,
                reason="initial-daily",
            )
        )

    def queue_history(self, task: HistoryTask) -> None:
        key = (
            task.symbol,
            task.resolution,
            task.start_ts,
            task.end_ts,
        )

        with self.history_pending_lock:
            if key in self.history_pending:
                return
            self.history_pending.add(key)

        self.history_queue.put(task)

    def history_worker(self) -> None:
        while not self.stop_event.is_set():
            try:
                task = self.history_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            key = (
                task.symbol,
                task.resolution,
                task.start_ts,
                task.end_ts,
            )

            try:
                self.process_history_task(task)
            except Exception as exc:
                msg = sanitize_message(exc)
                log.exception("History task failed: %s", msg)
                self.send_line(
                    f"ERROR\tHISTORY\t{task.symbol} {task.resolution}: {msg}"
                )
            finally:
                with self.history_pending_lock:
                    self.history_pending.discard(key)

                self.history_queue.task_done()

    def rest(self) -> fyersModel.FyersModel:
        with self.rest_lock:
            if self.fyers_rest is None:
                if not self.refresh_clients_from_token_file():
                    raise RuntimeError("No valid FYERS access token is available.")

            assert self.fyers_rest is not None
            return self.fyers_rest

    def process_history_task(self, task: HistoryTask) -> None:
        fy = self.rest()

        if task.resolution == "D":
            self.fetch_daily_chunked(fy, task)
        else:
            self.fetch_seconds(fy, task)

        self.send_line(
            f"HIST_DONE\t{task.symbol}\t{task.resolution}"
        )

    def fetch_seconds(
        self,
        fy: fyersModel.FyersModel,
        task: HistoryTask,
    ) -> None:
        params = {
            "symbol": task.symbol,
            "resolution": "5S",
            "date_format": "0",
            "range_from": str(int(task.start_ts)),
            "range_to": str(int(task.end_ts)),
            "cont_flag": "1",
            "oi_flag": "1",
        }

        response = fy.history(data=params)
        self.ensure_history_ok(task, response)

        candles = response.get("candles") or []
        self.emit_history(task.symbol, "5S", candles)

        log.info(
            "5S history: %s | %d candle(s) | %s",
            task.symbol,
            len(candles),
            task.reason,
        )

    def fetch_daily_chunked(
        self,
        fy: fyersModel.FyersModel,
        task: HistoryTask,
    ) -> None:
        current = task.start_ts

        while current <= task.end_ts and not self.stop_event.is_set():
            chunk_end = min(
                task.end_ts,
                current + DAILY_CHUNK_DAYS * 86400,
            )

            start_date = time.strftime(
                "%Y-%m-%d",
                time.localtime(current),
            )
            end_date = time.strftime(
                "%Y-%m-%d",
                time.localtime(chunk_end),
            )

            params = {
                "symbol": task.symbol,
                "resolution": "D",
                "date_format": "1",
                "range_from": start_date,
                "range_to": end_date,
                "cont_flag": "1",
                "oi_flag": "1",
            }

            response = fy.history(data=params)
            self.ensure_history_ok(task, response)

            self.emit_history(
                task.symbol,
                "D",
                response.get("candles") or [],
            )

            next_start = chunk_end + 86400

            if next_start <= current:
                break

            current = next_start

    @staticmethod
    def ensure_history_ok(
        task: HistoryTask,
        response: Any,
    ) -> None:
        if not isinstance(response, dict):
            raise RuntimeError(
                f"FYERS history returned an unexpected response: {response!r}"
            )

        if response.get("s") != "ok":
            raise RuntimeError(
                f"FYERS history rejected {task.symbol}/{task.resolution}: {response}"
            )

    def emit_history(
        self,
        symbol: str,
        resolution: str,
        candles: list[Any],
    ) -> None:
        batch: list[str] = []

        for candle in candles:
            if not isinstance(candle, (list, tuple)) or len(candle) < 6:
                continue

            ts = safe_int(candle[0])

            if ts <= 0:
                continue

            o = safe_float(candle[1])
            h = safe_float(candle[2])
            l = safe_float(candle[3])
            c = safe_float(candle[4])
            volume = safe_float(candle[5])
            oi = safe_float(candle[6]) if len(candle) >= 7 else 0.0

            batch.append(
                "HIST\t{}\t{}\t{}\t{:.10g}\t{:.10g}\t{:.10g}\t{:.10g}\t{:.10g}\t{:.10g}\n".format(
                    symbol,
                    resolution,
                    ts,
                    o,
                    h,
                    l,
                    c,
                    volume,
                    oi,
                )
            )

            if len(batch) >= HISTORY_BATCH_ROWS:
                self.send_history_batch(batch)
                batch.clear()

        if batch:
            self.send_history_batch(batch)

    def send_history_batch(self, lines: list[str]) -> None:
        payload = "".join(lines).encode("utf-8")

        with self.ipc_lock:
            sock = self.ipc_socket
            if sock is None:
                return

            try:
                sock.sendall(payload)
            except OSError:
                pass

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------
    def test_rest(self) -> None:
        try:
            fy = self.rest()

            with self.symbol_lock:
                symbols = sorted(self.symbols)

            symbol = symbols[0] if symbols else "NSE:NIFTY50-INDEX"

            response = fy.quotes({"symbols": symbol})

            if isinstance(response, dict) and response.get("s") == "ok":
                self.send_line(
                    f"TESTREST_OK\tFYERS REST quotes succeeded for {symbol}."
                )
            else:
                self.send_line(
                    f"TESTREST_ERR\tFYERS REST response: {response}"
                )
        except Exception as exc:
            self.send_line(
                f"TESTREST_ERR\tFYERS REST test failed: {sanitize_message(exc, 600)}"
            )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        if not self.app_id:
            raise SystemExit("--app-id is required")

        setup_logging(os.path.dirname(os.path.abspath(__file__)))

        for index in range(HISTORY_WORKERS):
            thread = threading.Thread(
                target=self.history_worker,
                name=f"FyersBridge-History-{index + 1}",
                daemon=True,
            )
            thread.start()
            self.history_threads.append(thread)

        self.ipc_thread.start()
        self.ws_thread.start()

        log.info("FyersBridge started")
        log.info("FYERS App ID: %s", self.app_id)
        log.info("Access token file: %s", self.token_path)
        log.info("No order/trading API is used")

        try:
            while not self.stop_event.is_set():
                try:
                    mtime = os.path.getmtime(self.token_path)
                except OSError:
                    mtime = 0.0

                # atok.py replaces the file with a new daily token. Resetting
                # the client here lets the websocket supervisor reload it.
                if mtime and self.token_mtime and mtime != self.token_mtime:
                    log.info("access_token.txt changed; rebuilding FYERS clients")
                    self.disconnect_ws()
                    with self.rest_lock:
                        self.fyers_rest = None
                    self.token_value = ""
                    self.token_mtime = 0.0

                time.sleep(TOKEN_POLL_SEC)

        except KeyboardInterrupt:
            log.info("KeyboardInterrupt received")

        finally:
            self.stop_event.set()
            self.disconnect_ws()

            with self.ipc_lock:
                sock = self.ipc_socket
                self.ipc_socket = None

            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

            log.info("FyersBridge stopped")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Official FYERS Python market-data bridge for AmiBroker"
    )

    parser.add_argument(
        "--app-id",
        required=True,
        help="Exact FYERS App ID including -100/-200 suffix.",
    )

    parser.add_argument(
        "--token-file",
        default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "access_token.txt",
        ),
        help="Daily access token file generated by atok.py.",
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Local bridge host.",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=47321,
        help="Local bridge TCP port.",
    )

    return parser.parse_args()


if __name__ == "__main__":
    arguments = parse_args()
    FyersBridge(arguments).start()
