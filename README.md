# FYERS Python Bridge 5-Second AmiBroker Data Plugin

This branch keeps the AmiBroker data plug-in native while moving all FYERS communication to the official FYERS Python API v3 SDK.

## Architecture

```
AmiBroker
   |
OpenAlgo.dll
   |
127.0.0.1:47321
   |
FyersBridge.py
   |   | +-- official FYERS DataSocket
   | +-- REST /data/history
   |
FYERS
```

The DLL does not make any FYERS HTTP or WebSocket calls.

There is **no CSV in the live path** and no OpenAlgo server is required.

## Authentication

The existing user workflow is intentionally preserved:

1. Run the existing `atok.py`.
2. Complete the normal FYERS browser login.
3. Paste the redirected localhost URL into `atok.py` as you already do.
4. `atok.py` writes `access_token.txt`.
5. The bridge reads that file.

The bridge never launches a browser and never performs the authorization-code exchange.

A temporary WebSocket/network disconnect does **not** regenerate the access token.

When `access_token.txt` changes, the DLL watchdog restarts the bridge so the new daily token is used.

Do not commit `access_token.txt`, the FYERS secret, or any other credential.

## Live market data

The bridge uses the official FYERS Python `FyersDataSocket` with `SymbolUpdate` and reconnect enabled.

Current FYERS message types include:
- `if` for index updates
- `sf` for equity/option updates
- `dp` for depth

The bridge explicitly handles index and tradable symbols differently for timestamps and volume.

Tradable equity/F&O volume is derived from FYERS cumulative traded-volume updates and last-traded quantity. Index data does not invent traded volume.

## 5-second engine

Live updates are bucketed by exchange timestamp into five-second intervals.

Each symbol maintains:
- current forming 5-second OHLCV/OI
- completed 5-second bars
- last completed timestamp
- cumulative-volume baseline

The DLL receives current-bar updates without writing them to disk.

At a new five-second bucket, the previous bar is marked complete.

## Gap repair

When the live exchange timestamp jumps over one or more five-second buckets, the bridge automatically queues a REST `5S` history request for the missing interval.

On local bridge restart, the DLL gives the bridge the last completed five-second timestamp so only the missing range needs to be requested.

The automatic gap-fill safety cap is 3600 seconds. A larger outage is reported instead of fabricating bars.

## Historical data

For a new symbol, the bridge requests:
- recent `5S` history
- approximately two years of `D` history

FYERS currently documents:
- seconds history: latest 30 trading days
- minute history: maximum 100 days/request
- day/week/month history: maximum 366 days/request

The bridge requests a slightly wider 42-calendar-day seconds window and retains whatever FYERS actually returns.

Daily history is chunked automatically.

## AmiBroker side

The DLL:
- implements the AmiBroker plugin exports
- stores bars in memory
- serves `GetQuotesEx`
- maintains `RecentInfo`
- posts AmiBroker streaming-update notifications
- starts and watches `FyersBridge.py`
- reconnects to the local bridge after a bridge/network failure

AmiBroker is never blocked waiting for a FYERS HTTP request.

## Performance goal

The old workflow used:

`FYERS -> Python -> CSV -> OLE import -> AmiBroker`

This branch removes the live CSV/OLE loop:

`FYERS WebSocket -> Python memory -> localhost IPC -> DLL memory -> AmiBroker`

Historical data uses REST only in the Python bridge and is streamed to the DLL over local IPC.

## Python setup

Install the official FYERS v3 Python package:

```
pip install -r requirements-fyers-bridge.txt
```

The bridge can also be run manually for diagnostics:

```
py -3 FyersBridge.py --app-id YOUR_FYERS_APP_ID-100 --token-file access_token.txt
```

When launched by the DLL, these arguments are supplied automatically.

## Configuration

The plug-in configuration dialog uses:
- **App ID**: exact FYERS App ID as issued
- **Token file**: normally `access_token.txt`
- **Gap check**: monitoring interval

The local configuration file contains no access token:

`FyersBridge.config`

## Analysis-only boundary

This branch is deliberately market-data-only.

It does not import or call the FYERS order socket and does not implement place, modify, cancel, GTT, position-management, or other order execution functions.
