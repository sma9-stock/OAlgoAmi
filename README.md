# FYERS Direct 5-Second AmiBroker Data Plugin

This fork changes the data path from OpenAlgo to a native FYERS REST + WebSocket transport.

## Target data model
- Base chart data: 5-second
- Initial daily history: 2 years
- Initial 5-second history: provider-available recent window
- Live feed: FYERS SymbolUpdate WebSocket
- Gap repair: REST 5-second candles after reconnects / detected time gaps
- No order placement or trading API is used

## Credentials
Configure the plugin with FYERS App ID and FYERS Access Token. The token is stored using Windows DPAPI under the current Windows user profile and is never embedded in source.

The existing access_token.txt file is accepted only as migration convenience; native configuration is preferred.

## AmiBroker database
Use a **5-second base interval**. AmiBroker can compress 5-second data into higher intraday intervals.

For the requested history, set **Number of bars to load** to at least **180,000** (200,000 is a comfortable setting). The plugin keeps up to 180,000 completed 5-second bars per symbol in its in-memory cache.

In **Intraday Settings**, enable **Allow mixed EOD/intraday data** if you want the same database to expose the older daily history together with the recent 5-second history.

Do not use Tick as the base interval for this build: the plugin supplies native 5-second candles, not raw tick storage.

## Build
Visual Studio 2022, Desktop development with C++, MFC, and Windows SDK. GitHub Actions builds Release|x64 and publishes the DLL artifact.

## History window
FYERS currently documents seconds-history availability as a recent **30-trading-day** window. The plugin requests a 42-calendar-day range, which is bounded by at most 30 weekdays before exchange holidays are considered, then stores the candles returned by FYERS.

## Limitation
FYERS cannot provide historical 5-second candles that its API does not expose. The plugin repairs gaps only when FYERS REST can return the requested 5-second data.
