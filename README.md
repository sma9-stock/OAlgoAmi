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
Use a 5-second base interval. AmiBroker can compress 5-second data into higher intervals. Do not use Tick base interval for this build: the plugin supplies 5-second bars, not raw tick storage.

## Build
Visual Studio 2022, Desktop development with C++, MFC, and Windows SDK. GitHub Actions builds Release|x64 and publishes the DLL artifact.

## Limitation
FYERS cannot provide historical 5-second candles that its API does not expose. The plugin repairs gaps only when FYERS REST can return the requested 5-second data.
