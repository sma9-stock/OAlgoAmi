#!/usr/bin/env python3
"""
FyersSmokeTest.py
-----------------
Independent FYERS API v3 diagnostic.

This does not touch AmiBroker and does not call any order/trading API.
It uses the same access_token.txt contract as atok.py/FyersBridge.py.

Examples:
    py -3 FyersSmokeTest.py --app-id YOUR_APP_ID-100
    py -3 FyersSmokeTest.py --app-id YOUR_APP_ID-100 --symbol NSE:CGPOWER-EQ
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from typing import Any

from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws


DEFAULT_SYMBOLS = [
    "NSE:NIFTY50-INDEX",
    "NSE:CGPOWER-EQ",
]

DEFAULT_OPTION_SYMBOL = ""


def read_token(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        token = handle.read().strip()
    if not token:
        raise RuntimeError("access_token.txt is empty")
    return token


def print_response(label: str, value: Any) -> None:
    try:
        print(f"{label}: {json.dumps(value, indent=2, default=str)}")
    except Exception:
        print(f"{label}: {value!r}")


def run_rest(app_id: str, token: str, symbols: list[str]) -> bool:
    print("\n" + "=" * 78)
    print("REST / QUOTES TEST")
    print("=" * 78)

    client = fyersModel.FyersModel(
        client_id=app_id,
        token=token,
        is_async=False,
        log_path="",
    )

    ok = True

    for symbol in symbols:
        try:
            response = client.quotes({"symbols": symbol})
            print_response(f"{symbol} quotes", response)

            if not isinstance(response, dict) or response.get("s") != "ok":
                ok = False
        except Exception as exc:
            ok = False
            print(f"{symbol} quotes EXCEPTION: {exc}")

    return ok


def run_history(
    app_id: str,
    token: str,
    symbols: list[str],
) -> bool:
    print("\n" + "=" * 78)
    print("5-SECOND HISTORY TEST")
    print("=" * 78)

    client = fyersModel.FyersModel(
        client_id=app_id,
        token=token,
        is_async=False,
        log_path="",
    )

    end_ts = int(time.time())
    start_ts = end_ts - 120

    all_ok = True

    for symbol in symbols:
        params = {
            "symbol": symbol,
            "resolution": "5S",
            "date_format": "0",
            "range_from": str(start_ts),
            "range_to": str(end_ts),
            "cont_flag": "1",
            "oi_flag": "1",
        }

        try:
            response = client.history(data=params)

            if not isinstance(response, dict):
                all_ok = False
                print(f"{symbol}: unexpected response {response!r}")
                continue

            candles = response.get("candles") or []

            print(
                f"{symbol}: status={response.get('s')} "
                f"code={response.get('code')} "
                f"candles={len(candles)}"
            )

            if response.get("s") == "ok":
                if candles:
                    print(f"  first={candles[0]}")
                    print(f"  last ={candles[-1]}")
                else:
                    print("  no candles in requested 120-second window (this can be normal outside live market activity)")
            else:
                all_ok = False
                print_response(f"  full response", response)

        except Exception as exc:
            all_ok = False
            print(f"{symbol}: EXCEPTION: {exc}")

    return all_ok


class WebSocketProbe:
    def __init__(self, app_id: str, token: str, symbols: list[str]) -> None:
        self.app_id = app_id
        self.token = token
        self.symbols = symbols
        self.messages = 0
        self.by_type: dict[str, int] = {}
        self.last_by_symbol: dict[str, dict[str, Any]] = {}
        self.connected = threading.Event()
        self.done = threading.Event()
        self.ws = None

    def on_connect(self) -> None:
        print("\nWS: authenticated/connected")
        self.connected.set()

        self.ws.subscribe(
            symbols=self.symbols,
            data_type="SymbolUpdate",
            channel=15,
        )

        print("WS: subscribed:", ", ".join(self.symbols))

    def on_message(self, message: Any) -> None:
        values = message if isinstance(message, list) else [message]

        for value in values:
            if not isinstance(value, dict):
                continue

            msg_type = str(value.get("type") or "?")
            self.by_type[msg_type] = self.by_type.get(msg_type, 0) + 1

            symbol = value.get("symbol")
            if symbol in self.symbols:
                self.messages += 1
                self.last_by_symbol[str(symbol)] = value

    def on_error(self, message: Any) -> None:
        print("WS ERROR:", message)

    def on_close(self, message: Any) -> None:
        print("WS CLOSE:", message)
        self.done.set()

    def run(self, seconds: int) -> bool:
        auth = f"{self.app_id}:{self.token}"

        self.ws = data_ws.FyersDataSocket(
            access_token=auth,
            log_path="",
            litemode=False,
            write_to_file=False,
            reconnect=True,
            on_connect=self.on_connect,
            on_close=self.on_close,
            on_error=self.on_error,
            on_message=self.on_message,
        )

        error_box: list[BaseException] = []

        def worker() -> None:
            try:
                self.ws.connect()
                self.ws.keep_running()
            except BaseException as exc:
                error_box.append(exc)
                self.done.set()

        thread = threading.Thread(
            target=worker,
            name="FyersSmokeTest-WS",
            daemon=True,
        )
        thread.start()

        connected = self.connected.wait(timeout=12)

        if not connected:
            print("WS: did not authenticate within 12 seconds")
            return False

        deadline = time.time() + seconds
        while time.time() < deadline and not self.done.is_set():
            time.sleep(0.2)

        try:
            self.ws.disconnect()
        except Exception:
            pass

        if error_box:
            print("WS EXCEPTION:", error_box[0])
            return False

        print("\nWS SUMMARY")
        print("messages:", self.messages)
        print("types:", self.by_type)

        for symbol in self.symbols:
            value = self.last_by_symbol.get(symbol)
            if value:
                print(f"last[{symbol}]: {value}")
            else:
                print(f"last[{symbol}]: NO UPDATE")

        # A successful authenticated connection is the WebSocket PASS condition.
        # Zero messages can be normal outside active market hours.
        return True


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Independent FYERS v3 market-data smoke test"
    )
    parser.add_argument(
        "--app-id",
        required=True,
        help="Exact FYERS App ID, including -100/-200 suffix.",
    )
    parser.add_argument(
        "--token-file",
        default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "access_token.txt",
        ),
    )
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        help="Symbol to test. May be supplied multiple times.",
    )
    parser.add_argument(
        "--option-symbol",
        default=DEFAULT_OPTION_SYMBOL,
        help="Optional F&O symbol such as NSE:NIFTY26OCT22700PE.",
    )
    parser.add_argument(
        "--ws-seconds",
        type=int,
        default=10,
        help="Seconds to observe live WebSocket messages.",
    )

    args = parser.parse_args()

    symbols = args.symbols or list(DEFAULT_SYMBOLS)

    if args.option_symbol.strip():
        symbols.append(args.option_symbol.strip())

    # Preserve order while deduplicating.
    symbols = list(dict.fromkeys(s.strip() for s in symbols if s.strip()))

    token = read_token(os.path.abspath(args.token_file))

    print("=" * 78)
    print("FYERS API v3 — INDEPENDENT MARKET-DATA SMOKE TEST")
    print("No AmiBroker connection. No CSV. No order/trading API.")
    print("Symbols:", ", ".join(symbols))
    print("=" * 78)

    rest_ok = run_rest(args.app_id, token, symbols)
    history_ok = run_history(args.app_id, token, symbols)

    print("\n" + "=" * 78)
    print("WEBSOCKET SYMBOLUPDATE TEST")
    print("=" * 78)

    probe = WebSocketProbe(args.app_id, token, symbols)
    ws_ok = probe.run(max(1, args.ws_seconds))

    print("\n" + "=" * 78)
    print("FINAL RESULT")
    print("=" * 78)
    print("REST quotes :", "PASS" if rest_ok else "FAIL")
    print("5S history  :", "PASS" if history_ok else "FAIL")
    print("WebSocket   :", "PASS" if ws_ok else "FAIL")

    return 0 if (rest_ok and history_ok and ws_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
