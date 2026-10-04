// OpenAlgoGlobals.h - Global variables and functions shared across the plugin
#ifndef OPENALGO_GLOBALS_H
#define OPENALGO_GLOBALS_H

#include "stdafx.h"
#include "resource.h"

// Status enum
enum OpenAlgoStatus
{
	STATUS_WAIT = 0,
	STATUS_CONNECTED = 1,
	STATUS_DISCONNECTED = 2,
	STATUS_SHUTDOWN = 3
};

// Legacy OpenAlgo settings remain declared for source compatibility. The
// FYERS-direct build does not start the legacy OpenAlgo transport.
extern HWND g_hAmiBrokerWnd;
extern int g_nPortNumber;
extern int g_nRefreshInterval;
extern int g_nBackfillRefreshIntervalSec;
extern int g_nTimeShift;
extern CString g_oServer;
extern CString g_oApiKey;
extern CString g_oWebSocketUrl;
extern int g_nStatus;

// FYERS direct mode
extern BOOL g_bDirectFyersMode;
extern CString g_fyersAppId;
extern CString g_fyersAccessToken;
extern CString g_fyersTokenFilePath;
extern int g_fyersGapCheckIntervalSec;
extern CString g_fyersLastError;

// Legacy backfill tracking
extern int g_nBackfillDays;
extern int g_nBackfillPeriodicity;
extern BOOL g_bBackfillRequested;

// Legacy real-time candle setting retained for compatibility
extern BOOL g_bRealTimeCandlesEnabled;
extern int g_nBackfillIntervalMs;

// Legacy OpenAlgo HTTP cache declarations
extern CMapStringToPtr g_HttpResponseCache;
extern CRITICAL_SECTION g_HttpCacheCriticalSection;
extern const DWORD HTTP_CACHE_LIFETIME_MS;

// Legacy helpers
CString GetAvailableSymbols(void);
CString BuildOpenAlgoURL(const CString& server, int port, const CString& endpoint);

BOOL WriteApiKeyDirect(const CString& key);
BOOL ReadApiKeyDirect(CString& outKey);

// Forward declarations from Plugin.h (Plugin.cpp includes this header first).
struct PluginNotification;
struct Quotation;
struct RecentInfo;

// FYERS direct API
BOOL FyersDirectInit(void);
void FyersDirectRelease(void);
BOOL FyersDirectNotify(struct PluginNotification* pn);
BOOL FyersDirectGetQuotesEx(LPCTSTR pszTicker, int nPeriodicity, int nLastValid,
                            int nSize, struct Quotation* pQuotes);
struct RecentInfo* FyersDirectGetRecentInfo(LPCTSTR pszTicker);
BOOL FyersDirectReconfigure(CString appId, CString tokenFilePath);
BOOL FyersTestRestConnection(void);
BOOL FyersTestWebSocket(void);
void FyersRequestReconnect(void);

#endif // OPENALGO_GLOBALS_H
