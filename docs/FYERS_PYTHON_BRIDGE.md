# FYERS Python Bridge architecture

## Purpose

This branch keeps the AmiBroker plug-in native but moves all FYERS-specific communication to the official FYERS Python SDK.

The live path is:

AmiBroker -> OpenAlgo.dll -> localhost TCP -> FyersBridge.py -> FYERS

There is no CSV file in the live data path and no OpenAlgo server is required.

## Authentication

The existing `atok.py` remains unchanged.

Run it once each trading day using the same folder as the bridge so it writes:

`access_token.txt`

The bridge reads that file. It does not open a browser, does not exchange authorization codes, and does not need the FYERS secret ID after the token has been generated.

When `access_token.txt` changes, the DLL watchdog restarts the bridge so the new daily token is picked up.

A temporary WebSocket disconnect does not invoke the browser login flow.

## Live data

The official FYERS Python data socket is used with `SymbolUpdate`.

Current FYERS documentation distinguishes:
- `if`: index update
- `sf`: equity/option update
- `dp`: depth update

The bridge deliberately treats index symbols differently when choosing the exchange timestamp and volume handling. Tradable equity/F&O symbols use FYERS cumulative traded volume and last-traded quantity to construct 5-second volume.

## 5-second engine

Live updates are bucketed by exchange timestamp:

`bucket = floor(timestamp / 5) * 5`

The bridge maintains:
- current forming 5-second OHLCV/OI
- completed 5-second bars
- last completed timestamp
- per-symbol cumulative-volume baseline

Out-of-order updates never overwrite a newer bar.

## Gap repair

When a new live timestamp jumps over one or more 5-second buckets, the missing range is queued to FYERS REST `/data/history` with resolution `5S`.

The maximum automatic gap-fill request is one hour. This prevents an accidental overnight outage from creating an enormous burst of requests; larger failures are reported rather than silently fabricated.

On process reconnect, the DLL sends each symbol's last completed 5-second timestamp. The bridge then fetches only the missing 5-second range.

## Historical load

For a new symbol the bridge requests:
- recent seconds-resolution history using `5S`
- approximately two years of daily history, chunked to FYERS' documented daily request limit

FYERS currently documents seconds history as limited to the latest 30 trading days, minute history to 100 days/request, and day/week/month history to 366 days/request.

The bridge requests a 42-calendar-day seconds window so FYERS can return its provider-available 30-trading-day window.

## AmiBroker side

The DLL:
- owns the AmiBroker plugin API
- owns an in-memory cache
- serves `GetQuotesEx` from memory
- maintains `RecentInfo`
- notifies AmiBroker when new data arrives
- launches and watches the Python bridge
- reconnects to the bridge after local process restarts

The DLL never calls a FYERS HTTP or WebSocket endpoint.

## Local IPC

The bridge is bound only to:

`127.0.0.1:47321`

The protocol is line-based and intentionally simple so the C++ DLL has no third-party JSON dependency.

This IPC is not a disk/file transport. Data stays in memory and crosses a local socket.

## Analysis-only boundary

This branch is market-data-only.

No order socket, order endpoint, place/modify/cancel operation, positions API, or trading command is used by the bridge implementation.
