// FyersDirect.inc
// -----------------------------------------------------------------------------
// Native FYERS market-data transport for the AmiBroker data plugin.
//
// Design:
//   * No Python process and no OpenAlgo server are required for market data.
//   * REST history: 2 years of EOD + latest FYERS-supported 5-second history.
//   * Live: one native WinHTTP WebSocket connection for all subscribed symbols.
//   * Reconnect: automatic, with 5-second history gap repair.
//   * Credentials: App ID + Access Token are stored locally; Access Token is
//     encrypted with Windows DPAPI. No credentials are ever committed to Git.
//
// FYERS current data API:
//   REST  https://api-t1.fyers.in/data
//   Data WS  wss://socket.fyers.in/hsm/v1-5/prod
//   Auth  Authorization: <app_id>:<access_token>
// -----------------------------------------------------------------------------

#include <winhttp.h>
#include <wincrypt.h>
#include <vector>
#include <algorithm>
#include <cmath>
#include <stdint.h>
#pragma comment(lib, "winhttp.lib")
#pragma comment(lib, "crypt32.lib")

#define FYERS_DIRECT_HOST              _T("api-t1.fyers.in")
#define FYERS_DIRECT_HOST_W            L"api-t1.fyers.in"
#define FYERS_DIRECT_WS_HOST           _T("socket.fyers.in")
#define FYERS_DIRECT_WS_HOST_W         L"socket.fyers.in"
#define FYERS_DIRECT_WS_PATH           _T("/hsm/v1-5/prod")
#define FYERS_DIRECT_WS_PATH_W         L"/hsm/v1-5/prod"
#define FYERS_DIRECT_HISTORY_PATH      _T("/data/history")
#define FYERS_DIRECT_QUOTES_PATH       _T("/data/quotes")
#define FYERS_DIRECT_SYMBOL_TOKEN_PATH _T("/data/symbol-token")

#define FYERS_PERIOD_5SEC              5
#define FYERS_PERIOD_DAILY             86400

// FYERS exposes seconds history only for the most recent 30 trading days.
// We deliberately ask a slightly wider calendar range for the initial load;
// the provider will cap the result to the available seconds-history window.
#define FYERS_5S_INITIAL_LOOKBACK_DAYS 42
#define FYERS_5S_FALLBACK_LOOKBACK_DAYS 42
#define FYERS_DAILY_LOOKBACK_DAYS      730
#define FYERS_MAX_5S_BARS              180000
#define FYERS_MAX_DAILY_BARS           1000
#define FYERS_RECONNECT_DELAY_MS       3000
#define FYERS_WS_RECEIVE_TIMEOUT_MS    1000
#define FYERS_WS_HEARTBEAT_SEC         10
#define FYERS_GAP_TRIGGER_SEC          10
#define FYERS_DAILY_REFRESH_MS         (60ULL * 60ULL * 1000ULL)

struct FyersCandle
{
	time_t ts;
	float open;
	float high;
	float low;
	float close;
	float volume;
	float oi;

	FyersCandle() : ts(0), open(0), high(0), low(0), close(0), volume(0), oi(0) {}
};

struct FyersHistoryJob
{
	CString ticker;
	int periodicity;
	time_t startTs;
	time_t endTs;
	BOOL initialLoad;

	FyersHistoryJob() : periodicity(0), startTs(0), endTs(0), initialLoad(FALSE) {}
};

struct FyersHsmSnapshot
{
	float ltp;
	float totalVol;
	float tradeQty;
	float oi;
	float bid;
	float ask;
	float open;
	float high;
	float low;
	float prevClose;
	time_t lastTradeTime;
	time_t exchangeFeedTime;
	double priceScale;
	BOOL valid;

	FyersHsmSnapshot()
		: ltp(0), totalVol(0), tradeQty(0), oi(0), bid(0), ask(0),
		  open(0), high(0), low(0), prevClose(0),
		  lastTradeTime(0), exchangeFeedTime(0), priceScale(100.0),
		  valid(FALSE)
	{}
};

struct FyersSymbolState
{
	CString amiTicker;
	CString fyersSymbol;
	CString hsmToken;
	WORD topicId;
	FyersHsmSnapshot snapshot;

	CArray<Quotation, Quotation> fiveSecBars;
	CArray<Quotation, Quotation> dailyBars;

	Quotation currentBar;
	BOOL currentBarStarted;
	time_t currentBarStart;

	time_t lastCompleted5s;
	time_t lastTradeTs;
	float lastTotalVolume;
	BOOL haveTotalVolume;

	ULONGLONG lastFiveSecFetchTick;
	ULONGLONG lastDailyFetchTick;
	BOOL fiveSecFetchInProgress;
	BOOL dailyFetchInProgress;
	BOOL seededFromAmi;
	BOOL startupGapChecked;
	BOOL subscribed;

	DWORD lastLocalTickTick;

	FyersSymbolState()
		: currentBarStarted(FALSE),
		  currentBarStart(0),
		  lastCompleted5s(0),
		  lastTradeTs(0),
		  lastTotalVolume(0.0f),
		  haveTotalVolume(FALSE),
		  lastFiveSecFetchTick(0),
		  lastDailyFetchTick(0),
		  fiveSecFetchInProgress(FALSE),
		  dailyFetchInProgress(FALSE),
		  seededFromAmi(FALSE),
		  startupGapChecked(FALSE),
		  subscribed(FALSE),
		  lastLocalTickTick(0)
	{
		memset(&currentBar, 0, sizeof(currentBar));
	}
};

static CMap<CString, LPCTSTR, FyersSymbolState*, FyersSymbolState*> g_fyersStates;
static CRITICAL_SECTION g_fyersStateCS;
static BOOL g_fyersStateCSInitialized = FALSE;

static CMap<CString, LPCTSTR, struct RecentInfo*, struct RecentInfo*> g_fyersRecentInfo;
static CRITICAL_SECTION g_fyersRecentInfoCS;
static BOOL g_fyersRecentInfoCSInitialized = FALSE;

static CList<FyersHistoryJob, FyersHistoryJob&> g_fyersHistoryQueue;
static CRITICAL_SECTION g_fyersHistoryQueueCS;
static BOOL g_fyersHistoryQueueCSInitialized = FALSE;
static HANDLE g_hFyersHistoryEvent = NULL;
static CWinThread* g_pFyersHistoryThread = NULL;
static volatile LONG g_bFyersHistoryShouldStop = 0;

static HINTERNET g_hFyersWebSocket = NULL;
static HINTERNET g_hFyersWsSession = NULL;
static HINTERNET g_hFyersWsConnection = NULL;
static CRITICAL_SECTION g_fyersWsCS;
static BOOL g_fyersWsCSInitialized = FALSE;
static CRITICAL_SECTION g_fyersWsIoCS;
static BOOL g_fyersWsIoCSInitialized = FALSE;
static CWinThread* g_pFyersWsThread = NULL;
static volatile LONG g_bFyersWsShouldStop = 0;
static volatile LONG g_bFyersForceReconnect = 0;
static HANDLE g_hFyersConnectedEvent = NULL;
static HANDLE g_hFyersAuthenticatedEvent = NULL;
static CString g_fyersLastError;

static CString FyersTrim(CString value)
{
	value.Trim();
	return value;
}

static CString FyersNormalizeAppId(CString appId)
{
	appId.Trim();
	// Preserve the App ID exactly as issued by FYERS. Current apps may use
	// suffixes such as -100 or -200; never guess or rewrite the ID.
	return appId;
}

static CString FyersNormalizeAccessToken(CString token)
{
	token.Trim();
	// Accept raw JWT or APP_ID:TOKEN for convenience, but store/use only JWT.
	int colon = token.Find(_T(':'));
	if (colon > 0 && token.Find(_T(':'), colon + 1) < 0 &&
		(token.Left(colon).Find(_T('-')) >= 0))
	{
		token = token.Mid(colon + 1);
		token.Trim();
	}
	return token;
}

static void FyersResetStatesForDatabase(void)
{
	if (!g_fyersStateCSInitialized)
		return;

	EnterCriticalSection(&g_fyersStateCS);

	POSITION pos = g_fyersStates.GetStartPosition();
	while (pos != NULL)
	{
		CString key;
		FyersSymbolState* state = NULL;
		g_fyersStates.GetNextAssoc(pos, key, state);
		if (!state)
			continue;

		state->fiveSecBars.RemoveAll();
		state->dailyBars.RemoveAll();
		state->hsmToken.Empty();
		state->topicId = 0;
		state->snapshot = FyersHsmSnapshot();
		memset(&state->currentBar, 0, sizeof(state->currentBar));
		state->currentBarStarted = FALSE;
		state->currentBarStart = 0;
		state->lastCompleted5s = 0;
		state->lastTradeTs = 0;
		state->lastTotalVolume = 0.0f;
		state->haveTotalVolume = FALSE;
		state->lastFiveSecFetchTick = 0;
		state->lastDailyFetchTick = 0;
		state->fiveSecFetchInProgress = FALSE;
		state->dailyFetchInProgress = FALSE;
		state->seededFromAmi = FALSE;
		state->startupGapChecked = FALSE;
		state->subscribed = FALSE;
		state->lastLocalTickTick = 0;
	}

	LeaveCriticalSection(&g_fyersStateCS);
}

static CString FyersNormalizeSymbol(LPCTSTR pszTicker)
{
	CString ticker(pszTicker ? pszTicker : _T(""));
	ticker.Trim();

	// Best case: AmiBroker symbol already contains the exact FYERS API symbol,
	// e.g. NSE:SBIN-EQ, NSE:NIFTY50-INDEX, NSE:NIFTY26OCT22700PE.
	if (ticker.Find(_T(':')) >= 0)
		return ticker;

	if (ticker.IsEmpty())
		return ticker;

	// Preserve the common -INDEX / -EQ convention while adding the exchange.
	CString upper = ticker;
	upper.MakeUpper();

	if (upper.Right(6) == _T("-INDEX") || upper.Right(3) == _T("-EQ"))
		return _T("NSE:") + ticker;

	// Legacy plugin style: SYMBOL-NSE / SYMBOL-BSE / SYMBOL-MCX.
	int dash = ticker.ReverseFind(_T('-'));
	if (dash > 0)
	{
		CString suffix = upper.Mid(dash + 1);
		if (suffix == _T("NSE") || suffix == _T("BSE") || suffix == _T("MCX"))
			return suffix == _T("BSE") ? _T("BSE:") + ticker.Left(dash)
			                           : suffix == _T("MCX") ? _T("MCX:") + ticker.Left(dash)
			                                                 : _T("NSE:") + ticker.Left(dash);
	}

	// Default to NSE because the user's existing AmiBroker/FYERS scripts use
	// NSE symbols for equities, indexes, and index options.
	return _T("NSE:") + ticker;
}

static CString FyersMakeStateKey(LPCTSTR pszTicker)
{
	CString s(pszTicker ? pszTicker : _T(""));
	s.Trim();
	return s;
}

static BOOL FyersIsEodQuotation(const Quotation& q)
{
	return q.DateTime.PackDate.Hour == DATE_EOD_HOURS &&
	       q.DateTime.PackDate.Minute == DATE_EOD_MINUTES;
}

static int FyersCompareYmd(const AmiDate& a, const AmiDate& b)
{
	if (a.PackDate.Year < b.PackDate.Year) return -1;
	if (a.PackDate.Year > b.PackDate.Year) return 1;
	if (a.PackDate.Month < b.PackDate.Month) return -1;
	if (a.PackDate.Month > b.PackDate.Month) return 1;
	if (a.PackDate.Day < b.PackDate.Day) return -1;
	if (a.PackDate.Day > b.PackDate.Day) return 1;
	return 0;
}

static time_t FyersQuotationTimestamp(const Quotation& q)
{
	struct tm t = {0};
	t.tm_year = (int)q.DateTime.PackDate.Year - 1900;
	t.tm_mon  = (int)q.DateTime.PackDate.Month - 1;
	t.tm_mday = (int)q.DateTime.PackDate.Day;
	t.tm_hour = (int)q.DateTime.PackDate.Hour;
	t.tm_min  = (int)q.DateTime.PackDate.Minute;
	t.tm_sec  = (int)q.DateTime.PackDate.Second;
	t.tm_isdst = -1;
	return mktime(&t);
}

static Quotation FyersMakeQuotation(const FyersCandle& c, BOOL bDaily)
{
	Quotation q;
	memset(&q, 0, sizeof(q));

	ConvertUnixToPackedDate(c.ts, &q.DateTime);

	if (bDaily)
	{
		q.DateTime.Date |= DAILY_MASK;
		q.DateTime.PackDate.Hour = DATE_EOD_HOURS;
		q.DateTime.PackDate.Minute = DATE_EOD_MINUTES;
		q.DateTime.PackDate.Second = 0;
		q.DateTime.PackDate.MilliSec = 0;
		q.DateTime.PackDate.MicroSec = 0;
	}

	q.Open = c.open;
	q.High = c.high;
	q.Low = c.low;
	q.Price = c.close;
	q.Volume = c.volume;
	q.OpenInterest = c.oi;
	q.AuxData1 = 0;
	q.AuxData2 = 0;
	return q;
}

static int FyersFindQuotationByTimestamp(const CArray<Quotation, Quotation>& arr, DATE_TIME_INT ts)
{
	int lo = 0;
	int hi = (int)arr.GetCount() - 1;

	while (lo <= hi)
	{
		int mid = lo + (hi - lo) / 2;
		DATE_TIME_INT v = arr[mid].DateTime.Date;
		if (v < ts) lo = mid + 1;
		else if (v > ts) hi = mid - 1;
		else return mid;
	}
	return -1;
}

static void FyersUpsertQuotation(CArray<Quotation, Quotation>& arr,
                                 const Quotation& q,
                                 int maxBars)
{
	DATE_TIME_INT key = q.DateTime.Date;
	int count = (int)arr.GetCount();

	if (count == 0 || key > arr[count - 1].DateTime.Date)
	{
		arr.Add(q);
	}
	else
	{
		int idx = FyersFindQuotationByTimestamp(arr, key);
		if (idx >= 0)
		{
			arr[idx] = q;
		}
		else
		{
			int lo = 0;
			int hi = count;
			while (lo < hi)
			{
				int mid = lo + (hi - lo) / 2;
				if (arr[mid].DateTime.Date < key) lo = mid + 1;
				else hi = mid;
			}
			arr.InsertAt(lo, q);
		}
	}

	if (maxBars > 0 && arr.GetCount() > maxBars)
	{
		int excess = (int)arr.GetCount() - maxBars;
		arr.RemoveAt(0, excess);
	}
}

static FyersSymbolState* FyersGetOrCreateState(LPCTSTR pszTicker)
{
	CString key = FyersMakeStateKey(pszTicker);
	if (key.IsEmpty())
		return NULL;

	EnterCriticalSection(&g_fyersStateCS);

	FyersSymbolState* state = NULL;
	if (!g_fyersStates.Lookup(key, state) || state == NULL)
	{
		state = new FyersSymbolState();
		state->amiTicker = key;
		state->fyersSymbol = FyersNormalizeSymbol(key);
		g_fyersStates.SetAt(key, state);
	}

	LeaveCriticalSection(&g_fyersStateCS);
	return state;
}

static void FyersClearStates(void)
{
	if (!g_fyersStateCSInitialized)
		return;

	EnterCriticalSection(&g_fyersStateCS);

	POSITION pos = g_fyersStates.GetStartPosition();
	while (pos != NULL)
	{
		CString key;
		FyersSymbolState* state = NULL;
		g_fyersStates.GetNextAssoc(pos, key, state);
		delete state;
	}
	g_fyersStates.RemoveAll();

	LeaveCriticalSection(&g_fyersStateCS);
}

static struct RecentInfo* FyersGetOrCreateRecentInfo(LPCTSTR pszTicker)
{
	CString ticker = FyersMakeStateKey(pszTicker);
	if (ticker.IsEmpty() || !g_fyersRecentInfoCSInitialized)
		return NULL;

	EnterCriticalSection(&g_fyersRecentInfoCS);

	struct RecentInfo* pInfo = NULL;
	if (!g_fyersRecentInfo.Lookup(ticker, pInfo) || pInfo == NULL)
	{
		pInfo = new struct RecentInfo();
		memset(pInfo, 0, sizeof(struct RecentInfo));
		pInfo->nStructSize = sizeof(struct RecentInfo);

		CStringA nameA(ticker);
		strncpy_s(pInfo->Name, sizeof(pInfo->Name), nameA, _TRUNCATE);

		CString fyersSymbol = FyersNormalizeSymbol(ticker);
		int colon = fyersSymbol.Find(_T(':'));
		CString exchange = colon > 0 ? fyersSymbol.Left(colon) : _T("NSE");
		CStringA exchA(exchange);
		strncpy_s(pInfo->Exchange, sizeof(pInfo->Exchange), exchA, _TRUNCATE);

		g_fyersRecentInfo.SetAt(ticker, pInfo);
	}

	LeaveCriticalSection(&g_fyersRecentInfoCS);
	return pInfo;
}

static void FyersUpdateRecentInfo(const CString& ticker,
                                  float ltp,
                                  float open,
                                  float high,
                                  float low,
                                  float prevClose,
                                  float volume,
                                  float oi,
                                  float bid,
                                  float ask,
                                  float tradeQty,
                                  BOOL isTrade)
{
	struct RecentInfo* pInfo = FyersGetOrCreateRecentInfo(ticker);
	if (!pInfo)
		return;

	EnterCriticalSection(&g_fyersRecentInfoCS);

	int bm = pInfo->nBitmap;
	if (ltp > 0)  { pInfo->fLast = ltp; bm |= RI_LAST; }
	if (open > 0) { pInfo->fOpen = open; bm |= RI_OPEN; }
	if (high > 0) { pInfo->fHigh = high; bm |= RI_HIGHLOW; }
	if (low > 0)  { pInfo->fLow = low; bm |= RI_HIGHLOW; }
	if (prevClose > 0)
	{
		pInfo->fPrev = prevClose;
		pInfo->fChange = ltp > 0 ? ltp - prevClose : pInfo->fChange;
		bm |= RI_PREVCHANGE;
	}
	if (volume >= 0)
	{
		pInfo->fTotalVol = volume;
		pInfo->iTotalVol = (int)volume;
		bm |= RI_TOTALVOL;
	}
	if (oi > 0)
	{
		pInfo->fOpenInt = oi;
		bm |= RI_OPENINT;
	}
	if (bid > 0) { pInfo->fBid = bid; bm |= RI_BID; }
	if (ask > 0) { pInfo->fAsk = ask; bm |= RI_ASK; }
	if (tradeQty > 0)
	{
		pInfo->fTradeVol = tradeQty;
		pInfo->iTradeVol = (int)tradeQty;
		bm |= RI_TRADEVOL;
	}

	CTime nowT = CTime::GetCurrentTime();
	pInfo->nDateUpdate = nowT.GetYear() * 10000 + nowT.GetMonth() * 100 + nowT.GetDay();
	pInfo->nTimeUpdate = nowT.GetHour() * 10000 + nowT.GetMinute() * 100 + nowT.GetSecond();
	bm |= RI_DATEUPDATE;

	if (isTrade)
	{
		pInfo->nDateChange = pInfo->nDateUpdate;
		pInfo->nTimeChange = pInfo->nTimeUpdate;
		bm |= RI_DATECHANGE;
		pInfo->nStatus = RI_STATUS_UPDATE | RI_STATUS_TRADE |
		                 RI_STATUS_BARSREADY;
	}
	else
	{
		pInfo->nStatus = RI_STATUS_UPDATE | RI_STATUS_BARSREADY;
	}

	pInfo->nBitmap = bm;

	LeaveCriticalSection(&g_fyersRecentInfoCS);

	if (g_hAmiBrokerWnd != NULL)
		PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, (LPARAM)pInfo);
}

static CStringA FyersUrlEncodeA(const CStringA& in)
{
	CStringA out;
	const char* p = in.GetString();

	for (int i = 0; i < in.GetLength(); i++)
	{
		unsigned char ch = (unsigned char)p[i];
		if ((ch >= 'A' && ch <= 'Z') ||
		    (ch >= 'a' && ch <= 'z') ||
		    (ch >= '0' && ch <= '9') ||
		    ch == '-' || ch == '_' || ch == '.' || ch == '~')
		{
			out += (char)ch;
		}
		else
		{
			char hex[4];
			sprintf_s(hex, sizeof(hex), "%%%02X", ch);
			out += hex;
		}
	}

	return out;
}

static CString FyersUrlEncode(CString value)
{
	return CString(FyersUrlEncodeA(CStringA(value)));
}


static CString FyersWinHttpErrorText(DWORD code)
{
\tchar buf[512] = {0};
\tDWORD n = FormatMessageA(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
\t                         NULL, code, 0, buf, (DWORD)sizeof(buf), NULL);
\tCString msg;
\tif (n > 0)
\t{
\t\tmsg = CString(buf);
\t\tmsg.Trim();
\t}
\tif (msg.IsEmpty())
\t\tmsg.Format(_T("WinHTTP error %lu"), (unsigned long)code);
\treturn msg;
}

static void FyersSetLastError(const CString& message)
{
\tg_fyersLastError = message;
\tOutputDebugString(message);
\tOutputDebugString(_T("\r\n"));
}

static void FyersSetWinHttpError(LPCTSTR where, DWORD code)
{
\tCString msg;
\tmsg.Format(_T("%s: %s (code %lu)"), where,
\t           (LPCTSTR)FyersWinHttpErrorText(code),
\t           (unsigned long)code);
\tFyersSetLastError(msg);
}

static BOOL FyersReadHttpResponse(HINTERNET hRequest, CStringA& response)
{
\tresponse.Empty();
\tfor (;;)
\t{
\t\tDWORD available = 0;
\t\tif (!WinHttpQueryDataAvailable(hRequest, &available))
\t\t{
\t\t\tFyersSetWinHttpError(_T("WinHttpQueryDataAvailable"), GetLastError());
\t\t\treturn FALSE;
\t\t}
\t\tif (!available) break;
\t\tstd::vector<char> buf((size_t)available + 1, 0);
\t\tDWORD read = 0;
\t\tif (!WinHttpReadData(hRequest, buf.data(), available, &read))
\t\t{
\t\t\tFyersSetWinHttpError(_T("WinHttpReadData"), GetLastError());
\t\t\treturn FALSE;
\t\t}
\t\tif (read) response.Append(buf.data(), (int)read);
\t}
\treturn TRUE;
}

static BOOL FyersHttpGet(const CString& pathAndQuery, CStringA& response, DWORD& statusCode)
{
\tresponse.Empty();
\tstatusCode = 0;

\tif (g_fyersAppId.IsEmpty() || g_fyersAccessToken.IsEmpty())
\t{
\t\tFyersSetLastError(_T("FYERS REST: App ID or Access Token is empty."));
\t\treturn FALSE;
\t}

\tHINTERNET hSession=NULL,hConnect=NULL,hRequest=NULL;
\tBOOL ok=FALSE;
\tCStringW pathW(pathAndQuery), appIdW(FyersNormalizeAppId(g_fyersAppId)),
\t          tokenW(FyersNormalizeAccessToken(g_fyersAccessToken)), auth;
\tauth.Format(L"Authorization: %s:%s\r\n",(LPCWSTR)appIdW,(LPCWSTR)tokenW);

\thSession=WinHttpOpen(L"FYERS Direct AmiBroker Data Plugin/1.0",
\t\tWINHTTP_ACCESS_TYPE_AUTOMATIC_PROXY,WINHTTP_NO_PROXY_NAME,
\t\tWINHTTP_NO_PROXY_BYPASS,0);
\tif(!hSession){FyersSetWinHttpError(_T("WinHttpOpen"),GetLastError());goto cleanup;}
\tWinHttpSetTimeouts(hSession,5000,5000,10000,10000);

\thConnect=WinHttpConnect(hSession,FYERS_DIRECT_HOST_W,INTERNET_DEFAULT_HTTPS_PORT,0);
\tif(!hConnect){FyersSetWinHttpError(_T("WinHttpConnect(api-t1.fyers.in)"),GetLastError());goto cleanup;}

\thRequest=WinHttpOpenRequest(hConnect,L"GET",(LPCWSTR)pathW,NULL,
\tWINHTTP_NO_REFERER,WINHTTP_DEFAULT_ACCEPT_TYPES,WINHTTP_FLAG_SECURE);
\tif(!hRequest){FyersSetWinHttpError(_T("WinHttpOpenRequest"),GetLastError());goto cleanup;}

\t{
\t\tCStringW headers=auth;
\t\theaders+=L"Content-Type: application/json\r\nAccept: application/json\r\nversion: 3\r\n";
\t\tif(!WinHttpAddRequestHeaders(hRequest,(LPCWSTR)headers,(DWORD)-1L,
\t\t\tWINHTTP_ADDREQ_FLAG_ADD|WINHTTP_ADDREQ_FLAG_REPLACE))
\t\t{FyersSetWinHttpError(_T("WinHttpAddRequestHeaders"),GetLastError());goto cleanup;}
\t}

\tif(!WinHttpSendRequest(hRequest,WINHTTP_NO_ADDITIONAL_HEADERS,0,
\t\tWINHTTP_NO_REQUEST_DATA,0,0,0))
\t{FyersSetWinHttpError(_T("WinHttpSendRequest"),GetLastError());goto cleanup;}

\tif(!WinHttpReceiveResponse(hRequest,NULL))
\t{FyersSetWinHttpError(_T("WinHttpReceiveResponse"),GetLastError());goto cleanup;}

\t{
\t\tDWORD size=sizeof(statusCode);
\t\tif(!WinHttpQueryHeaders(hRequest,WINHTTP_QUERY_STATUS_CODE|WINHTTP_QUERY_FLAG_NUMBER,
\t\t\tWINHTTP_HEADER_NAME_BY_INDEX,&statusCode,&size,WINHTTP_NO_HEADER_INDEX))
\t\t{FyersSetWinHttpError(_T("WinHttpQueryHeaders"),GetLastError());goto cleanup;}
\t}

\tif(!FyersReadHttpResponse(hRequest,response)) goto cleanup;
\tok=(statusCode>=200 && statusCode<300);

\tif(!ok)
\t{
\t\tCString msg;
\t\tmsg.Format(_T("FYERS REST HTTP %lu"),(unsigned long)statusCode);
\t\tif(!response.IsEmpty()){CString p(response.Left(600));p.Trim();if(!p.IsEmpty()){msg+=_T(": ");msg+=p;}}
\t\tFyersSetLastError(msg);
\t}
\telse
\t\tFyersSetLastError(_T("FYERS REST: HTTP request succeeded."));

cleanup:
\tif(hRequest)WinHttpCloseHandle(hRequest);
\tif(hConnect)WinHttpCloseHandle(hConnect);
\tif(hSession)WinHttpCloseHandle(hSession);
\treturn ok;
}

static BOOL FyersHttpPostJson(const CString& path,const CStringA& body,
\tconst CStringA& authorization,CStringA& response,DWORD& statusCode)
{
\tresponse.Empty();statusCode=0;
\tif(authorization.IsEmpty()){FyersSetLastError(_T("FYERS POST: authorization token is empty."));return FALSE;}

\tHINTERNET hSession=NULL,hConnect=NULL,hRequest=NULL;BOOL ok=FALSE;
\tCStringW pathW(path),authW;
\tCStringW authSrc(CString(authorization));
\tauthW.Format(L"Authorization: %s\r\n",(LPCWSTR)authSrc);

\thSession=WinHttpOpen(L"FYERS Direct AmiBroker Plugin/1.0",WINHTTP_ACCESS_TYPE_AUTOMATIC_PROXY,
\t\tWINHTTP_NO_PROXY_NAME,WINHTTP_NO_PROXY_BYPASS,0);
\tif(!hSession){FyersSetWinHttpError(_T("WinHttpOpen(POST)"),GetLastError());goto cleanup;}
\tWinHttpSetTimeouts(hSession,5000,5000,10000,10000);
\thConnect=WinHttpConnect(hSession,FYERS_DIRECT_HOST_W,INTERNET_DEFAULT_HTTPS_PORT,0);
\tif(!hConnect){FyersSetWinHttpError(_T("WinHttpConnect(POST)"),GetLastError());goto cleanup;}
\thRequest=WinHttpOpenRequest(hConnect,L"POST",(LPCWSTR)pathW,NULL,WINHTTP_NO_REFERER,
\t\tWINHTTP_DEFAULT_ACCEPT_TYPES,WINHTTP_FLAG_SECURE);
\tif(!hRequest){FyersSetWinHttpError(_T("WinHttpOpenRequest(POST)"),GetLastError());goto cleanup;}

\t{
\t\tCStringW headers=authW;
\t\theaders+=L"Content-Type: application/json\r\nAccept: application/json\r\n";
\t\tif(!WinHttpAddRequestHeaders(hRequest,(LPCWSTR)headers,(DWORD)-1L,
\t\t\tWINHTTP_ADDREQ_FLAG_ADD|WINHTTP_ADDREQ_FLAG_REPLACE))
\t\t{FyersSetWinHttpError(_T("WinHttpAddRequestHeaders(POST)"),GetLastError());goto cleanup;}
\t}

\tCStringA payload=body;
\tif(!WinHttpSendRequest(hRequest,WINHTTP_NO_ADDITIONAL_HEADERS,0,(PVOID)payload.GetString(),
\t\t(DWORD)payload.GetLength(),(DWORD)payload.GetLength(),0))
\t{FyersSetWinHttpError(_T("WinHttpSendRequest(POST)"),GetLastError());goto cleanup;}

\tif(!WinHttpReceiveResponse(hRequest,NULL))
\t{FyersSetWinHttpError(_T("WinHttpReceiveResponse(POST)"),GetLastError());goto cleanup;}

\t{
\t\tDWORD size=sizeof(statusCode);
\t\tif(!WinHttpQueryHeaders(hRequest,WINHTTP_QUERY_STATUS_CODE|WINHTTP_QUERY_FLAG_NUMBER,
\t\t\tWINHTTP_HEADER_NAME_BY_INDEX,&statusCode,&size,WINHTTP_NO_HEADER_INDEX))
\t\t{FyersSetWinHttpError(_T("WinHttpQueryHeaders(POST)"),GetLastError());goto cleanup;}
\t}

\tif(!FyersReadHttpResponse(hRequest,response)) goto cleanup;
\tok=(statusCode>=200 && statusCode<300);
\tif(!ok)
\t{
\t\tCString msg;
\t\tmsg.Format(_T("FYERS POST HTTP %lu"),(unsigned long)statusCode);
\t\tif(!response.IsEmpty()){CString p(response.Left(600));p.Trim();if(!p.IsEmpty()){msg+=_T(": ");msg+=p;}}
\t\tFyersSetLastError(msg);
\t}
cleanup:
\tif(hRequest)WinHttpCloseHandle(hRequest);
\tif(hConnect)WinHttpCloseHandle(hConnect);
\tif(hSession)WinHttpCloseHandle(hSession);
\treturn ok;
}

static BOOL FyersParseNumber(const CStringA& json, int& pos, double& value)
{
	const char* base = json.GetString();
	int len = json.GetLength();

	while (pos < len && (base[pos] == ' ' || base[pos] == '\t' ||
	                     base[pos] == '\r' || base[pos] == '\n' ||
	                     base[pos] == ','))
	{
		pos++;
	}

	if (pos >= len)
		return FALSE;

	char* endPtr = NULL;
	value = strtod(base + pos, &endPtr);
	if (endPtr == base + pos)
		return FALSE;

	pos = (int)(endPtr - base);
	return TRUE;
}

static BOOL FyersParseCandles(const CStringA& json, std::vector<FyersCandle>& out)
{
	out.clear();

	int p = json.Find("\"candles\"");
	if (p < 0)
		return FALSE;

	p = json.Find('[', p);
	if (p < 0)
		return FALSE;
	p++;

	while (p < json.GetLength())
	{
		while (p < json.GetLength() &&
		       (json[p] == ' ' || json[p] == '\t' || json[p] == '\r' ||
		        json[p] == '\n' || json[p] == ','))
			p++;

		if (p >= json.GetLength() || json[p] == ']')
			break;

		if (json[p] != '[')
		{
			p++;
			continue;
		}
		p++;

		double n[7] = {0};
		BOOL valid = TRUE;
		for (int i = 0; i < 6; i++)
		{
			if (!FyersParseNumber(json, p, n[i]))
			{
				valid = FALSE;
				break;
			}
			while (p < json.GetLength() &&
			       (json[p] == ' ' || json[p] == '\t' || json[p] == '\r' ||
			        json[p] == '\n'))
				p++;
			if (i < 5 && p < json.GetLength() && json[p] == ',')
				p++;
		}

		// Optional 7th field: Open Interest when oi_flag=1.
		if (valid)
		{
			int q = p;
			while (q < json.GetLength() &&
			       (json[q] == ' ' || json[q] == '\t' || json[q] == '\r' ||
			        json[q] == '\n'))
				q++;
			if (q < json.GetLength() && json[q] == ',')
			{
				p = q + 1;
				if (!FyersParseNumber(json, p, n[6]))
					valid = FALSE;
			}
		}

		while (p < json.GetLength() && json[p] != ']')
			p++;
		if (p < json.GetLength())
			p++;

		if (!valid)
			continue;

		FyersCandle c;
		c.ts = (time_t)n[0];
		c.open = (float)n[1];
		c.high = (float)n[2];
		c.low = (float)n[3];
		c.close = (float)n[4];
		c.volume = (float)n[5];
		c.oi = (float)n[6];

		if (c.ts > 0)
			out.push_back(c);
	}

	return !out.empty();
}

static BOOL FyersFetchCandles(const CString& fyersSymbol,
                              int periodicity,
                              time_t startTs,
                              time_t endTs,
                              std::vector<FyersCandle>& out)
{
	out.clear();
	if (fyersSymbol.IsEmpty() || startTs <= 0 || endTs < startTs)
		return FALSE;

	CString resolution;
	BOOL useEpoch = FALSE;

	if (periodicity == FYERS_PERIOD_5SEC)
	{
		resolution = _T("5S");
		useEpoch = TRUE;
	}
	else if (periodicity == FYERS_PERIOD_DAILY)
	{
		resolution = _T("D");
	}
	else
	{
		return FALSE;
	}

	CString symbolParam = FyersUrlEncode(fyersSymbol);
	CString path;

	if (useEpoch)
	{
		path.Format(_T("%s?symbol=%s&resolution=%s&date_format=0&range_from=%lld&range_to=%lld&cont_flag=1&oi_flag=1"),
			FYERS_DIRECT_HISTORY_PATH,
			(LPCTSTR)symbolParam,
			(LPCTSTR)resolution,
			(long long)startTs,
			(long long)endTs);
	}
	else
	{
		CTime st(startTs);
		CTime et(endTs);
		path.Format(_T("%s?symbol=%s&resolution=%s&date_format=1&range_from=%s&range_to=%s&cont_flag=1&oi_flag=1"),
			FYERS_DIRECT_HISTORY_PATH,
			(LPCTSTR)symbolParam,
			(LPCTSTR)resolution,
			(LPCTSTR)st.Format(_T("%Y-%m-%d")),
			(LPCTSTR)et.Format(_T("%Y-%m-%d")));
	}

	CStringA body;
	DWORD status = 0;
	if (!FyersHttpGet(path, body, status))
	{
		if (periodicity == FYERS_PERIOD_5SEC && status != 0)
		{
			// A defensive fallback for providers/proxy paths that reject a
			// wider-than-allowed seconds range. Retry with 30 calendar days.
			time_t fallbackStart = max(startTs, endTs - (time_t)FYERS_5S_FALLBACK_LOOKBACK_DAYS * 86400);
			if (fallbackStart < startTs)
				fallbackStart = startTs;

			CString fallbackPath;
			fallbackPath.Format(_T("%s?symbol=%s&resolution=5S&date_format=0&range_from=%lld&range_to=%lld&cont_flag=1&oi_flag=1"),
				FYERS_DIRECT_HISTORY_PATH,
				(LPCTSTR)symbolParam,
				(long long)fallbackStart,
				(long long)endTs);

			body.Empty();
			status = 0;
			if (!FyersHttpGet(fallbackPath, body, status))
				return FALSE;
		}
		else
		{
			return FALSE;
		}
	}

	return FyersParseCandles(body, out);
}

static void FyersMergeCandles(FyersSymbolState* state,
                              const std::vector<FyersCandle>& candles,
                              int periodicity)
{
	if (!state || candles.empty())
		return;

	EnterCriticalSection(&g_fyersStateCS);

	if (periodicity == FYERS_PERIOD_5SEC)
	{
		if (state->fiveSecBars.GetCount() == 0)
		{
			state->fiveSecBars.SetSize((int)min((size_t)FYERS_MAX_5S_BARS, candles.size()));
			int write = 0;
			for (size_t i = candles.size() > (size_t)FYERS_MAX_5S_BARS
				? candles.size() - FYERS_MAX_5S_BARS : 0;
			     i < candles.size(); i++)
			{
				if (write >= FYERS_MAX_5S_BARS)
					break;
				state->fiveSecBars[write++] = FyersMakeQuotation(candles[i], FALSE);
			}
			if (write < state->fiveSecBars.GetCount())
				state->fiveSecBars.SetSize(write);
		}
		else
		{
			for (size_t i = 0; i < candles.size(); i++)
				FyersUpsertQuotation(state->fiveSecBars,
				                     FyersMakeQuotation(candles[i], FALSE),
				                     FYERS_MAX_5S_BARS);
		}

		if (state->fiveSecBars.GetCount() > 0)
			state->lastCompleted5s = FyersQuotationTimestamp(
				state->fiveSecBars[state->fiveSecBars.GetCount() - 1]);
	}
	else
	{
		if (state->dailyBars.GetCount() == 0)
		{
			state->dailyBars.SetSize((int)min((size_t)FYERS_MAX_DAILY_BARS, candles.size()));
			int write = 0;
			for (size_t i = candles.size() > (size_t)FYERS_MAX_DAILY_BARS
				? candles.size() - FYERS_MAX_DAILY_BARS : 0;
			     i < candles.size(); i++)
			{
				if (write >= FYERS_MAX_DAILY_BARS)
					break;
				state->dailyBars[write++] = FyersMakeQuotation(candles[i], TRUE);
			}
			if (write < state->dailyBars.GetCount())
				state->dailyBars.SetSize(write);
		}
		else
		{
			for (size_t i = 0; i < candles.size(); i++)
				FyersUpsertQuotation(state->dailyBars,
				                     FyersMakeQuotation(candles[i], TRUE),
				                     FYERS_MAX_DAILY_BARS);
		}
	}

	LeaveCriticalSection(&g_fyersStateCS);
}

static BOOL FyersSaveSettings(void)
{
	CString appId = FyersNormalizeAppId(g_fyersAppId);
	CString token = FyersTrim(g_fyersAccessToken);
	if (appId.IsEmpty() || token.IsEmpty())
		return FALSE;

	CString localAppData;
	{
		TCHAR buf[MAX_PATH] = {0};
		DWORD n = GetEnvironmentVariable(_T("LOCALAPPDATA"), buf, MAX_PATH);
		if (n > 0 && n < MAX_PATH)
			localAppData = buf;
	}
	if (localAppData.IsEmpty())
		return FALSE;

	CString dir = localAppData + _T("\\FyersAmiBroker");
	CreateDirectory(dir, NULL);

	DATA_BLOB plain = {0};
	DATA_BLOB encrypted = {0};
	plain.pbData = (BYTE*)(LPCTSTR)token;
	plain.cbData = (DWORD)((token.GetLength() + 1) * sizeof(TCHAR));

	if (!CryptProtectData(&plain,
	                      L"FYERS AmiBroker Direct Access Token",
	                      NULL, NULL, NULL,
	                      CRYPTPROTECT_UI_FORBIDDEN,
	                      &encrypted))
		return FALSE;

	CStringA hex;
	for (DWORD i = 0; i < encrypted.cbData; i++)
	{
		char b[3];
		sprintf_s(b, sizeof(b), "%02X", encrypted.pbData[i]);
		hex += b;
	}
	LocalFree(encrypted.pbData);

	CString path = dir + _T("\\settings.dat");
	CString tmp = path + _T(".tmp");

	HANDLE h = CreateFile(tmp, GENERIC_WRITE, 0, NULL, CREATE_ALWAYS,
	                      FILE_ATTRIBUTE_HIDDEN | FILE_ATTRIBUTE_NORMAL, NULL);
	if (h == INVALID_HANDLE_VALUE)
		return FALSE;

	CStringA content;
	content.Format("appid=%s\r\ntoken_dpapi=%s\r\n",
	               (LPCSTR)CStringA(appId),
	               (LPCSTR)hex);

	DWORD written = 0;
	BOOL ok = WriteFile(h, content.GetString(), (DWORD)content.GetLength(),
	                    &written, NULL);
	FlushFileBuffers(h);
	CloseHandle(h);

	if (!ok || written != (DWORD)content.GetLength())
	{
		DeleteFile(tmp);
		return FALSE;
	}

	if (!MoveFileEx(tmp, path, MOVEFILE_REPLACE_EXISTING))
	{
		DeleteFile(tmp);
		return FALSE;
	}

	return TRUE;
}

static BOOL FyersLoadSettings(void)
{
	g_fyersAppId.Empty();
	g_fyersAccessToken.Empty();

	CString localAppData;
	{
		TCHAR buf[MAX_PATH] = {0};
		DWORD n = GetEnvironmentVariable(_T("LOCALAPPDATA"), buf, MAX_PATH);
		if (n > 0 && n < MAX_PATH)
			localAppData = buf;
	}
	if (localAppData.IsEmpty())
		return FALSE;

	CString path = localAppData + _T("\\FyersAmiBroker\\settings.dat");
	HANDLE h = CreateFile(path, GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING,
	                      FILE_ATTRIBUTE_NORMAL | FILE_ATTRIBUTE_HIDDEN, NULL);
	if (h == INVALID_HANDLE_VALUE)
		return FALSE;

	std::vector<char> raw(8192, 0);
	DWORD read = 0;
	BOOL ok = ReadFile(h, raw.data(), (DWORD)raw.size() - 1, &read, NULL);
	CloseHandle(h);
	if (!ok || read == 0)
		return FALSE;

	CStringA text(raw.data(), (int)read);

	int appPos = text.Find("appid=");
	if (appPos >= 0)
	{
		appPos += 6;
		int end = text.Find('\n', appPos);
		if (end < 0) end = text.GetLength();
		CStringA app = text.Mid(appPos, end - appPos);
		app.Trim();
		g_fyersAppId = CString(app);
	}

	int tokPos = text.Find("token_dpapi=");
	if (tokPos < 0)
		return !g_fyersAppId.IsEmpty();

	tokPos += 12;
	int tokEnd = text.Find('\n', tokPos);
	if (tokEnd < 0) tokEnd = text.GetLength();
	CStringA hex = text.Mid(tokPos, tokEnd - tokPos);
	hex.Trim();

	if (hex.GetLength() < 2 || (hex.GetLength() % 2) != 0)
		return FALSE;

	std::vector<BYTE> blob((size_t)hex.GetLength() / 2);
	for (int i = 0; i < hex.GetLength(); i += 2)
	{
		char pair[3] = { hex[i], hex[i + 1], 0 };
		blob[(size_t)i / 2] = (BYTE)strtoul(pair, NULL, 16);
	}

	DATA_BLOB encrypted = {0};
	DATA_BLOB plain = {0};
	encrypted.pbData = blob.data();
	encrypted.cbData = (DWORD)blob.size();

	if (!CryptUnprotectData(&encrypted, NULL, NULL, NULL, NULL,
	                        CRYPTPROTECT_UI_FORBIDDEN, &plain))
		return FALSE;

	if (plain.pbData && plain.cbData > 0)
	{
		// The file was written using TCHAR-sized UTF-16 data in Unicode builds.
		g_fyersAccessToken = CString((LPCTSTR)plain.pbData);
	}
	if (plain.pbData)
		LocalFree(plain.pbData);

	g_fyersAppId = FyersNormalizeAppId(g_fyersAppId);
	return !g_fyersAppId.IsEmpty() && !g_fyersAccessToken.IsEmpty();
}

static BOOL FyersLoadLegacyAccessTokenFile(void)
{
	// Compatibility only: the user's existing scripts write access_token.txt.
	// The DLL itself does NOT launch Python or depend on those scripts at runtime.
	if (!g_fyersAppId.IsEmpty() && !g_fyersAccessToken.IsEmpty())
		return TRUE;

	CString path = _T("access_token.txt");
	CFileException ex;
	CStdioFile file;
	if (!file.Open(path, CFile::modeRead | CFile::typeText, &ex))
		return FALSE;

	CString line;
	if (file.ReadString(line))
	{
		line.Trim();
		if (line.Find(_T("key=")) == 0)
			line = line.Mid(4);
		g_fyersAccessToken = line;
	}
	file.Close();

	return !g_fyersAccessToken.IsEmpty();
}

static BOOL FyersCredentialsReady(void)
{
	if (g_fyersAppId.IsEmpty() || g_fyersAccessToken.IsEmpty())
		return FALSE;

	g_fyersAppId = FyersNormalizeAppId(g_fyersAppId);
	return TRUE;
}

BOOL FyersTestRestConnection(void)
{
\tCStringA response;DWORD status=0;CString symbol=FyersUrlEncode(_T("NSE:NIFTY50-INDEX"));
\tCString path;path.Format(_T("%s?symbols=%s"),FYERS_DIRECT_QUOTES_PATH,(LPCTSTR)symbol);
\tif(!FyersHttpGet(path,response,status))return FALSE;
\tCString s=ExtractJsonStringValue(CString(response),_T("s"));
\tif(s.CompareNoCase(_T("ok"))!=0){CString m=ExtractJsonStringValue(CString(response),_T("message"));if(m.IsEmpty())m=_T("FYERS returned a non-ok response.");FyersSetLastError(m);return FALSE;}
\tFyersSetLastError(_T("FYERS REST: authentication + quotes request succeeded."));return TRUE;
}

static void FyersQueueHistoryJob(const CString& ticker,int periodicity,time_t startTs,time_t endTs,BOOL initialLoad);

static uint16_t FyersReadBE16(const BYTE* data,size_t len,size_t& off,BOOL& ok)
{
\tif(!data||off+2>len){ok=FALSE;return 0;}
\tuint16_t v=(uint16_t)(((uint16_t)data[off]<<8)|data[off+1]);off+=2;return v;
}
static uint32_t FyersReadBE32(const BYTE* data,size_t len,size_t& off,BOOL& ok)
{
\tif(!data||off+4>len){ok=FALSE;return 0;}
\tuint32_t v=((uint32_t)data[off]<<24)|((uint32_t)data[off+1]<<16)|
\t           ((uint32_t)data[off+2]<<8)|(uint32_t)data[off+3];off+=4;return v;
}
static int32_t FyersReadBEI32(const BYTE* data,size_t len,size_t& off,BOOL& ok)
{
\treturn (int32_t)FyersReadBE32(data,len,off,ok);
}
static BOOL FyersReadHsmString(const BYTE* data,size_t len,size_t& off,CStringA& out)
{
\tout.Empty();if(!data||off>=len)return FALSE;BYTE n=data[off++];if(off+n>len)return FALSE;
\tif(n)out.Append((LPCSTR)(data+off),(int)n);off+=n;return TRUE;
}

static BOOL FyersSendWebSocketBinary(const BYTE* data,DWORD size)
{
\tif(!data||size==0||!g_fyersWsIoCSInitialized||!g_fyersWsCSInitialized)return FALSE;
\tBOOL result=FALSE;
\tEnterCriticalSection(&g_fyersWsIoCS);
\tHINTERNET ws=NULL;
\tEnterCriticalSection(&g_fyersWsCS);ws=g_hFyersWebSocket;LeaveCriticalSection(&g_fyersWsCS);
\tif(ws)
\t{
\t\tDWORD rc=WinHttpWebSocketSend(ws,WINHTTP_WEB_SOCKET_BINARY_MESSAGE_BUFFER_TYPE,(PVOID)data,size);
\t\tresult=(rc==NO_ERROR);
\t\tif(!result)FyersSetWinHttpError(_T("WinHttpWebSocketSend"),rc);
\t}
\tLeaveCriticalSection(&g_fyersWsIoCS);
\treturn result;
}
static BOOL FyersSendWebSocketBinary(const std::vector<BYTE>& data)
{
\treturn data.empty()?FALSE:FyersSendWebSocketBinary(data.data(),(DWORD)data.size());
}

static int FyersBase64UrlValue(char c)
{
\tif(c>='A'&&c<='Z')return c-'A';if(c>='a'&&c<='z')return c-'a'+26;
\tif(c>='0'&&c<='9')return c-'0'+52;if(c=='-')return 62;if(c=='_')return 63;return -1;
}
static BOOL FyersBase64UrlDecode(const CStringA& in,std::vector<BYTE>& out)
{
\tout.clear();int acc=0,bits=0;
\tfor(int i=0;i<in.GetLength();i++){char ch=in[i];if(ch=='=')break;int v=FyersBase64UrlValue(ch);if(v<0)return FALSE;
\t\tacc=(acc<<6)|v;bits+=6;if(bits>=8){bits-=8;out.push_back((BYTE)((acc>>bits)&0xFF));}}
\treturn !out.empty();
}
static BOOL FyersExtractHsmKeyFromAccessToken(CString& hsmKey)
{
\thsmKey.Empty();CString token=FyersNormalizeAccessToken(g_fyersAccessToken);
\tint d1=token.Find(_T('.')),d2=d1>=0?token.Find(_T('.'),d1+1):-1;
\tif(d1<=0||d2<=d1+1){FyersSetLastError(_T("FYERS WebSocket: Access Token is not a 3-part JWT."));return FALSE;}
\tCStringA payloadB64(CStringA(token.Mid(d1+1,d2-d1-1)));std::vector<BYTE> decoded;
\tif(!FyersBase64UrlDecode(payloadB64,decoded)){FyersSetLastError(_T("FYERS WebSocket: JWT payload decode failed."));return FALSE;}
\tdecoded.push_back(0);CString payload((LPCSTR)decoded.data());
\tCString expRaw=ExtractJsonRawValue(payload,_T("exp"));
\tif(!expRaw.IsEmpty()){__int64 exp=_tstoi64(expRaw);if(exp>0&&exp<(__int64)time(NULL)){FyersSetLastError(_T("FYERS WebSocket: Access Token is expired."));return FALSE;}}
\thsmKey=ExtractJsonStringValue(payload,_T("hsm_key"));
\tif(hsmKey.IsEmpty()){FyersSetLastError(_T("FYERS WebSocket: JWT has no hsm_key."));return FALSE;}
\treturn TRUE;
}

static CString FyersExchangeSegment(const CString& ex)
{
\tif(ex==_T("1010"))return _T("nse_cm");if(ex==_T("1011"))return _T("nse_fo");
\tif(ex==_T("1120"))return _T("mcx_fo");if(ex==_T("1210"))return _T("bse_cm");
\tif(ex==_T("1211"))return _T("bse_fo");if(ex==_T("1212"))return _T("bcs_fo");
\tif(ex==_T("1012"))return _T("cde_fo");if(ex==_T("1020"))return _T("nse_com");
\treturn CString();
}
static CString FyersIndexHsmName(const CString& symbol)
{
\tCString s=symbol;s.MakeUpper();struct Pair{LPCTSTR a;LPCTSTR b;};
\tstatic const Pair p[]={
\t\t{_T("NSE:NIFTY50-INDEX"),_T("Nifty 50")},{_T("NSE:NIFTYBANK-INDEX"),_T("Nifty Bank")},
\t\t{_T("NSE:FINNIFTY-INDEX"),_T("Nifty Fin Service")},{_T("NSE:MIDCPNIFTY-INDEX"),_T("NIFTY MID SELECT")},
\t\t{_T("NSE:NIFTYMIDSELECT-INDEX"),_T("NIFTY MID SELECT")},{_T("NSE:NIFTYNEXT50-INDEX"),_T("Nifty Next 50")},
\t\t{_T("NSE:NIFTYNXT50-INDEX"),_T("Nifty Next 50")},{_T("NSE:NIFTY100-INDEX"),_T("Nifty 100")},
\t\t{_T("NSE:NIFTY200-INDEX"),_T("Nifty 200")},{_T("NSE:NIFTY500-INDEX"),_T("Nifty 500")},
\t\t{_T("NSE:INDIAVIX-INDEX"),_T("India VIX")},{_T("NSE:NIFTYIT-INDEX"),_T("Nifty IT")},
\t\t{_T("NSE:NIFTYAUTO-INDEX"),_T("Nifty Auto")},{_T("NSE:NIFTYPHARMA-INDEX"),_T("Nifty Pharma")},
\t\t{_T("NSE:NIFTYMETAL-INDEX"),_T("Nifty Metal")},{_T("NSE:NIFTYFMCG-INDEX"),_T("Nifty FMCG")},
\t\t{_T("NSE:NIFTYREALTY-INDEX"),_T("Nifty Realty")},{_T("NSE:NIFTYENERGY-INDEX"),_T("Nifty Energy")},
\t\t{_T("NSE:NIFTYINFRA-INDEX"),_T("Nifty Infra")},{_T("NSE:NIFTYCPSE-INDEX"),_T("Nifty CPSE")},
\t\t{_T("NSE:NIFTYPSE-INDEX"),_T("Nifty PSE")},{_T("NSE:NIFTYPSUBANK-INDEX"),_T("Nifty PSU Bank")},
\t\t{_T("NSE:NIFTYCOMMODITIES-INDEX"),_T("Nifty Commodities")},{_T("NSE:NIFTYCONSUMPTION-INDEX"),_T("Nifty Consumption")}
\t};
\tfor(size_t i=0;i<sizeof(p)/sizeof(p[0]);i++)if(s==p[i].a)return p[i].b;
\tint colon=s.Find(_T(':'));if(colon>=0)s=s.Mid(colon+1);int d=s.Find(_T("-INDEX"));if(d>0)s=s.Left(d);return s;
}
static CString FyersExtractValidSymbolToken(const CStringA& response,const CString& symbol)
{
\tint p=response.Find("\"validSymbol\"");if(p<0)return CString();
\tCStringA needle;needle.Format("\"%s\"",(LPCSTR)CStringA(symbol));int k=response.Find(needle,p);if(k<0)return CString();
\tint colon=response.Find(':',k+needle.GetLength());if(colon<0)return CString();
\tint q1=response.Find('"',colon+1),q2=q1>=0?response.Find('"',q1+1):-1;if(q1<0||q2<=q1)return CString();
\treturn CString(response.Mid(q1+1,q2-q1-1));
}
static CString FyersBuildHsmToken(const CString& symbol,const CString& fyToken)
{
\tif(fyToken.GetLength()<4)return CString();CString seg=FyersExchangeSegment(fyToken.Left(4));if(seg.IsEmpty())return CString();
\tCString upper=symbol;upper.MakeUpper();BOOL index=(upper.Right(6)==_T("-INDEX"));
\tif(index){CString n=FyersIndexHsmName(symbol);CString h;h.Format(_T("if|%s|%s"),(LPCTSTR)seg,(LPCTSTR)n);return h;}
\tCString exToken;if(fyToken.GetLength()>10)exToken=fyToken.Mid(10);
\tif(exToken.IsEmpty()){int d=fyToken.ReverseFind(_T('|'));if(d>=0)exToken=fyToken.Mid(d+1);}
\tif(exToken.IsEmpty())return CString();CString h;h.Format(_T("sf|%s|%s"),(LPCTSTR)seg,(LPCTSTR)exToken);return h;
}

static BOOL FyersSendHsmSubscribe(const std::vector<CString>& hsmTokens)
{
\tif(hsmTokens.empty())return TRUE;std::vector<BYTE> sd;sd.push_back(0);sd.push_back(0);
\tfor(size_t i=0;i<hsmTokens.size();i++){CStringA s(hsmTokens[i]);if(s.GetLength()>255){FyersSetLastError(_T("FYERS WebSocket: HSM token too long."));return FALSE;}
\t\tsd.push_back((BYTE)s.GetLength());sd.insert(sd.end(),(BYTE*)s.GetString(),(BYTE*)s.GetString()+s.GetLength());}
\tsd[0]=(BYTE)(hsmTokens.size()>>8);sd[1]=(BYTE)(hsmTokens.size()&0xFF);
\tsize_t total=18+sd.size();if(total>65535){FyersSetLastError(_T("FYERS WebSocket: subscription packet too large."));return FALSE;}
\tstd::vector<BYTE> msg(total,0);uint16_t dl=(uint16_t)(total-2);msg[0]=(BYTE)(dl>>8);msg[1]=(BYTE)dl;
\tmsg[2]=4;msg[3]=2;msg[4]=1;msg[5]=(BYTE)(sd.size()>>8);msg[6]=(BYTE)sd.size();memcpy(msg.data()+7,sd.data(),sd.size());
\tsize_t off=7+sd.size();msg[off++]=2;msg[off++]=0;msg[off++]=1;msg[off++]=11;
\treturn FyersSendWebSocketBinary(msg);
}

static BOOL FyersSubscribeAll(void)
{
\tif(!FyersCredentialsReady())return FALSE;std::vector<CString> tickers;
\tEnterCriticalSection(&g_fyersStateCS);POSITION pos=g_fyersStates.GetStartPosition();
\twhile(pos){CString k;FyersSymbolState* s=NULL;g_fyersStates.GetNextAssoc(pos,k,s);if(s&&s->subscribed)tickers.push_back(s->amiTicker);}
\tLeaveCriticalSection(&g_fyersStateCS);if(tickers.empty())return TRUE;

\tfor(size_t base=0;base<tickers.size();base+=100)
\t{
\t\tsize_t count=min((size_t)100,tickers.size()-base);CStringA body("{\"symbols\":[");
\t\tfor(size_t i=0;i<count;i++){if(i)body+=",";CStringA sym(tickers[base+i]);body+="\"";body+=sym;body+="\"";}body+="]}";
\t\tCStringA response;DWORD status=0;
\t\tif(!FyersHttpPostJson(FYERS_DIRECT_SYMBOL_TOKEN_PATH,body,CStringA(g_fyersAccessToken),response,status))
\t\t\treturn FALSE;
\t\tCString st=ExtractJsonStringValue(CString(response),_T("s"));
\t\tif(st.CompareNoCase(_T("ok"))!=0){CString m=ExtractJsonStringValue(CString(response),_T("message"));if(m.IsEmpty())m=_T("FYERS /data/symbol-token returned an error.");FyersSetLastError(m);return FALSE;}

\t\tstd::vector<CString> hsm;
\t\tfor(size_t i=0;i<count;i++)
\t\t{
\t\t\tCString ticker=tickers[base+i],fy=FyersExtractValidSymbolToken(response,ticker),hs=FyersBuildHsmToken(ticker,fy);
\t\t\tif(hs.IsEmpty()){CString m;m.Format(_T("FYERS: could not convert symbol to HSM token: %s"),(LPCTSTR)ticker);FyersSetLastError(m);continue;}
\t\t\tEnterCriticalSection(&g_fyersStateCS);FyersSymbolState* s=NULL;if(g_fyersStates.Lookup(ticker,s)&&s){s->hsmToken=hs;s->topicId=0;}LeaveCriticalSection(&g_fyersStateCS);
\t\t\thsm.push_back(hs);
\t\t}
\t\tfor(size_t i=0;i<hsm.size();i+=500){size_t n=min((size_t)500,hsm.size()-i);std::vector<CString> chunk(hsm.begin()+i,hsm.begin()+i+n);if(!FyersSendHsmSubscribe(chunk))return FALSE;}
\t}
\tFyersSetLastError(_T("FYERS WebSocket: subscription request sent."));
\treturn TRUE;
}

static BOOL FyersEnsureSubscribed(LPCTSTR pszTicker)
{
\tFyersSymbolState* state=FyersGetOrCreateState(pszTicker);if(!state)return FALSE;BOOL fresh=FALSE;
\tEnterCriticalSection(&g_fyersStateCS);if(!state->subscribed){state->subscribed=TRUE;fresh=TRUE;}LeaveCriticalSection(&g_fyersStateCS);
\tif(fresh&&g_hFyersAuthenticatedEvent&&WaitForSingleObject(g_hFyersAuthenticatedEvent,0)==WAIT_OBJECT_0)FyersSubscribeAll();
\treturn TRUE;
}

static BOOL FyersParseTimestampField(const CString& raw,time_t& outTs)
{
\tCString s=raw;s.Trim();if(s.IsEmpty())return FALSE;__int64 v=_tstoi64(s);if(v>1000000000000LL)v/=1000;if(v<1000000000LL)return FALSE;outTs=(time_t)v;return TRUE;
}
static FyersSymbolState* FyersFindStateByFyersSymbol(LPCTSTR sym)
{
\tif(!sym||!*sym||!g_fyersStateCSInitialized)return NULL;FyersSymbolState* r=NULL;EnterCriticalSection(&g_fyersStateCS);
\tPOSITION p=g_fyersStates.GetStartPosition();while(p){CString k;FyersSymbolState* s=NULL;g_fyersStates.GetNextAssoc(p,k,s);if(s&&s->fyersSymbol.CompareNoCase(sym)==0){r=s;break;}}LeaveCriticalSection(&g_fyersStateCS);return r;
}
static FyersSymbolState* FyersFindStateByHsmToken(LPCTSTR token)
{
\tif(!token||!*token||!g_fyersStateCSInitialized)return NULL;FyersSymbolState* r=NULL;EnterCriticalSection(&g_fyersStateCS);
\tPOSITION p=g_fyersStates.GetStartPosition();while(p){CString k;FyersSymbolState* s=NULL;g_fyersStates.GetNextAssoc(p,k,s);if(s&&s->hsmToken.CompareNoCase(token)==0){r=s;break;}}LeaveCriticalSection(&g_fyersStateCS);return r;
}
static FyersSymbolState* FyersFindStateByTopicId(WORD id)
{
\tif(!id||!g_fyersStateCSInitialized)return NULL;FyersSymbolState* r=NULL;EnterCriticalSection(&g_fyersStateCS);
\tPOSITION p=g_fyersStates.GetStartPosition();while(p){CString k;FyersSymbolState* s=NULL;g_fyersStates.GetNextAssoc(p,k,s);if(s&&s->topicId==id){r=s;break;}}LeaveCriticalSection(&g_fyersStateCS);return r;
}

static void FyersHandleMarketMessage(const CString& message)
{
\tif(message.Find(_T("\"symbol\""))<0||message.Find(_T("\"ltp\""))<0)return;CString ticker=ExtractJsonStringValue(message,_T("symbol"));if(ticker.IsEmpty())return;
\tfloat ltp=ExtractJsonFloatValue(message,_T("ltp")),open=ExtractJsonFloatValue(message,_T("open_price")),
\t\thigh=ExtractJsonFloatValue(message,_T("high_price")),low=ExtractJsonFloatValue(message,_T("low_price")),
\t\tprev=ExtractJsonFloatValue(message,_T("prev_close_price")),total=ExtractJsonFloatValue(message,_T("vol_traded_today")),
\t\tqty=ExtractJsonFloatValue(message,_T("last_traded_qty")),oi=ExtractJsonFloatValue(message,_T("oi")),
\t\tbid=ExtractJsonFloatValue(message,_T("bid_price")),ask=ExtractJsonFloatValue(message,_T("ask_price"));
\tCString raw=ExtractJsonRawValue(message,_T("last_traded_time"));if(raw.IsEmpty())raw=ExtractJsonRawValue(message,_T("exch_feed_time"));
\ttime_t tradeTs=0;if(!FyersParseTimestampField(raw,tradeTs)||tradeTs<=0)tradeTs=time(NULL);if(ltp<=0)return;
\tFyersSymbolState* state=FyersFindStateByFyersSymbol(ticker);if(!state)return;CString ami=state->amiTicker;
\ttime_t barStart=(tradeTs/FYERS_PERIOD_5SEC)*FYERS_PERIOD_5SEC,gapStart=0,gapEnd=0;BOOL isTrade=TRUE;
\tEnterCriticalSection(&g_fyersStateCS);
\tBOOL gap=FALSE;if(state->currentBarStarted&&barStart>state->currentBarStart+FYERS_PERIOD_5SEC){gap=TRUE;gapStart=state->currentBarStart+FYERS_PERIOD_5SEC;gapEnd=barStart-FYERS_PERIOD_5SEC;}
\tfloat vd=0.0f;if(total>0.0f){if(state->haveTotalVolume&&total>=state->lastTotalVolume)vd=total-state->lastTotalVolume;else if(!state->haveTotalVolume)vd=qty;state->lastTotalVolume=total;state->haveTotalVolume=TRUE;}else vd=qty;
\tif(gap)vd=qty;BOOL dup=(state->lastTradeTs==tradeTs&&ltp==state->currentBar.Price&&fabs((double)vd)<0.0001);if(dup)isTrade=FALSE;
\tif(!state->currentBarStarted||barStart!=state->currentBarStart){if(state->currentBarStarted&&state->currentBarStart>0){FyersUpsertQuotation(state->fiveSecBars,state->currentBar,FYERS_MAX_5S_BARS);state->lastCompleted5s=state->currentBarStart;}
\t\tmemset(&state->currentBar,0,sizeof(state->currentBar));ConvertUnixToPackedDate(barStart,&state->currentBar.DateTime);state->currentBar.Open=ltp;state->currentBar.High=ltp;state->currentBar.Low=ltp;state->currentBar.Price=ltp;state->currentBar.Volume=max(0.0f,vd);state->currentBar.OpenInterest=oi;state->currentBarStart=barStart;state->currentBarStarted=TRUE;
\t}else{if(ltp>state->currentBar.High)state->currentBar.High=ltp;if(ltp<state->currentBar.Low||state->currentBar.Low==0.0f)state->currentBar.Low=ltp;state->currentBar.Price=ltp;state->currentBar.Volume+=max(0.0f,vd);if(oi>0)state->currentBar.OpenInterest=oi;}
\tstate->lastTradeTs=tradeTs;state->lastLocalTickTick=(DWORD)GetTickCount64();LeaveCriticalSection(&g_fyersStateCS);
\tFyersUpdateRecentInfo(ami,ltp,open,high,low,prev,total,oi,bid,ask,qty,isTrade);
\tif(gapStart>0&&gapEnd>=gapStart){FyersHistoryJob job;job.ticker=ami;job.periodicity=FYERS_PERIOD_5SEC;job.startTs=gapStart;job.endTs=gapEnd;job.initialLoad=FALSE;BOOL q=FALSE;
\t\tEnterCriticalSection(&g_fyersStateCS);if(!state->fiveSecFetchInProgress){state->fiveSecFetchInProgress=TRUE;q=TRUE;}LeaveCriticalSection(&g_fyersStateCS);
\t\tif(q&&g_fyersHistoryQueueCSInitialized){EnterCriticalSection(&g_fyersHistoryQueueCS);g_fyersHistoryQueue.AddTail(job);LeaveCriticalSection(&g_fyersHistoryQueueCS);if(g_hFyersHistoryEvent)SetEvent(g_hFyersHistoryEvent);}
\t}
\tif(g_hAmiBrokerWnd)PostMessage(g_hAmiBrokerWnd,WM_USER_STREAMING_UPDATE,0,0);
}

static void FyersUpdateHsmField(FyersSymbolState* state,BOOL index,int idx,int32_t value,BOOL& relevant)
{
\tif(!state||value==(int32_t)0x80000000)return;FyersHsmSnapshot& s=state->snapshot;
\tif(!index){switch(idx){case 0:s.ltp=(float)(value/s.priceScale);relevant=TRUE;break;case 1:s.totalVol=(float)value;relevant=TRUE;break;case 2:s.lastTradeTime=(time_t)value;relevant=TRUE;break;case 3:s.exchangeFeedTime=(time_t)value;break;case 6:s.bid=(float)(value/s.priceScale);relevant=TRUE;break;case 7:s.ask=(float)(value/s.priceScale);relevant=TRUE;break;case 8:s.tradeQty=(float)value;relevant=TRUE;break;case 12:s.oi=(float)value;relevant=TRUE;break;case 13:s.low=(float)(value/s.priceScale);relevant=TRUE;break;case 14:s.high=(float)(value/s.priceScale);relevant=TRUE;break;case 19:s.open=(float)(value/s.priceScale);relevant=TRUE;break;case 20:s.prevClose=(float)(value/s.priceScale);relevant=TRUE;break;default:break;}}
\telse{switch(idx){case 0:s.ltp=(float)(value/s.priceScale);relevant=TRUE;break;case 1:s.prevClose=(float)(value/s.priceScale);relevant=TRUE;break;case 2:s.exchangeFeedTime=(time_t)value;relevant=TRUE;break;case 3:s.high=(float)(value/s.priceScale);relevant=TRUE;break;case 4:s.low=(float)(value/s.priceScale);relevant=TRUE;break;case 5:s.open=(float)(value/s.priceScale);relevant=TRUE;break;default:break;}}
\tif(s.ltp>0)s.valid=TRUE;
}
static CString FyersSnapshotToJson(FyersSymbolState* state)
{
\tFyersHsmSnapshot s;CString sym;EnterCriticalSection(&g_fyersStateCS);s=state->snapshot;sym=state->fyersSymbol;LeaveCriticalSection(&g_fyersStateCS);
\tif(!s.valid||s.ltp<=0||sym.IsEmpty())return CString();CStringA a(sym);CStringA j;j.Format("{\"symbol\":\"%s\",\"ltp\":%.8f,\"vol_traded_today\":%.0f,\"last_traded_time\":%lld,\"exch_feed_time\":%lld,\"bid_price\":%.8f,\"ask_price\":%.8f,\"last_traded_qty\":%.0f,\"OI\":%.0f,\"oi\":%.0f,\"low_price\":%.8f,\"high_price\":%.8f,\"open_price\":%.8f,\"prev_close_price\":%.8f}",
\t\t(LPCSTR)a,s.ltp,s.totalVol,(long long)s.lastTradeTime,(long long)s.exchangeFeedTime,s.bid,s.ask,s.tradeQty,s.oi,s.oi,s.low,s.high,s.open,s.prevClose);
\treturn CString(j);
}
static void FyersEmitHsmSnapshot(FyersSymbolState* state){CString j=FyersSnapshotToJson(state);if(!j.IsEmpty())FyersHandleMarketMessage(j);}

static void FyersParseHsmDataFeed(const BYTE* data,size_t len)
{
\tif(!data||len<9)return;BOOL ok=TRUE;size_t off=0;FyersReadBE16(data,len,off,ok);if(!ok||off>=len)return;BYTE mt=data[off++];if(mt==6){if(off+4>len)return;off+=4;}
\tuint16_t count=FyersReadBE16(data,len,off,ok);if(!ok)return;
\tfor(uint16_t n=0;n<count&&off<len;n++){BYTE t=data[off++];
\t\tif(t==83){uint16_t topicId=FyersReadBE16(data,len,off,ok);if(!ok||off>=len)return;CStringA topicA;if(!FyersReadHsmString(data,len,off,topicA))return;CString topic(topicA);
\t\t\tBOOL index=topic.Left(3).CompareNoCase(_T("if|"))==0,scrip=topic.Left(3).CompareNoCase(_T("sf|"))==0;if(!index&&!scrip)return;FyersSymbolState* state=FyersFindStateByHsmToken(topic);if(!state)return;
\t\t\tEnterCriticalSection(&g_fyersStateCS);state->topicId=(WORD)topicId;state->snapshot=FyersHsmSnapshot();LeaveCriticalSection(&g_fyersStateCS);
\t\t\tif(off>=len)return;BYTE fc=data[off++];
\t\t\tfor(BYTE i=0;i<fc;i++){int32_t v=FyersReadBEI32(data,len,off,ok);if(!ok)return;EnterCriticalSection(&g_fyersStateCS);FyersHsmSnapshot& s=state->snapshot;
\t\t\t\tif(index){switch(i){case 0:s.ltp=(float)v;break;case 1:s.prevClose=(float)v;break;case 2:s.exchangeFeedTime=(time_t)v;break;case 3:s.high=(float)v;break;case 4:s.low=(float)v;break;case 5:s.open=(float)v;break;default:break;}}
\t\t\t\telse{switch(i){case 0:s.ltp=(float)v;break;case 1:s.totalVol=(float)v;break;case 2:s.lastTradeTime=(time_t)v;break;case 3:s.exchangeFeedTime=(time_t)v;break;case 6:s.bid=(float)v;break;case 7:s.ask=(float)v;break;case 8:s.tradeQty=(float)v;break;case 12:s.oi=(float)v;break;case 13:s.low=(float)v;break;case 14:s.high=(float)v;break;case 19:s.open=(float)v;break;case 20:s.prevClose=(float)v;break;default:break;}}\n\t\t\t\tLeaveCriticalSection(&g_fyersStateCS);}
\t\t\tif(off+2>len)return;off+=2;uint16_t mult=FyersReadBE16(data,len,off,ok);if(!ok||off>=len)return;BYTE prec=data[off++];CStringA tmp;if(!FyersReadHsmString(data,len,off,tmp))return;if(!FyersReadHsmString(data,len,off,tmp))return;if(!FyersReadHsmString(data,len,off,tmp))return;
\t\t\tdouble scale=pow(10.0,(double)prec)*(double)mult;if(scale<=0)scale=100.0;EnterCriticalSection(&g_fyersStateCS);FyersHsmSnapshot& s=state->snapshot;s.priceScale=scale;
\t\t\ts.ltp=(float)(s.ltp/scale);s.prevClose=(float)(s.prevClose/scale);s.high=(float)(s.high/scale);s.low=(float)(s.low/scale);s.open=(float)(s.open/scale);if(!index){s.bid=(float)(s.bid/scale);s.ask=(float)(s.ask/scale);}
\t\t\ts.valid=(s.ltp>0);LeaveCriticalSection(&g_fyersStateCS);FyersEmitHsmSnapshot(state);
\t\t}
\t\telse if(t==85){uint16_t topicId=FyersReadBE16(data,len,off,ok);if(!ok||off>=len)return;BYTE fc=data[off++];FyersSymbolState* state=FyersFindStateByTopicId((WORD)topicId);
\t\t\tif(!state){size_t skip=(size_t)fc*4;if(off+skip>len)return;off+=skip;continue;}CString topic;EnterCriticalSection(&g_fyersStateCS);topic=state->hsmToken;LeaveCriticalSection(&g_fyersStateCS);BOOL index=topic.Left(3).CompareNoCase(_T("if|"))==0,relevant=FALSE;
\t\t\tfor(BYTE i=0;i<fc;i++){int32_t v=FyersReadBEI32(data,len,off,ok);if(!ok)return;EnterCriticalSection(&g_fyersStateCS);FyersUpdateHsmField(state,index,i,v,relevant);LeaveCriticalSection(&g_fyersStateCS);}if(relevant)FyersEmitHsmSnapshot(state);
\t\t}
\t\telse if(t==76){uint16_t topicId=FyersReadBE16(data,len,off,ok);int32_t v=FyersReadBEI32(data,len,off,ok);if(!ok)return;FyersSymbolState* state=FyersFindStateByTopicId((WORD)topicId);if(state){EnterCriticalSection(&g_fyersStateCS);state->snapshot.ltp=(float)(v/state->snapshot.priceScale);state->snapshot.valid=TRUE;LeaveCriticalSection(&g_fyersStateCS);FyersEmitHsmSnapshot(state);}}
\t\telse return;
\t}
}
static void FyersProcessWebSocketPayload(const BYTE* data,size_t len){if(data&&len)FyersParseHsmDataFeed(data,len);}

static void FyersCloseWebSocketOnOwnerThread(void)
{
\tHINTERNET ws=NULL,conn=NULL,session=NULL;EnterCriticalSection(&g_fyersWsIoCS);EnterCriticalSection(&g_fyersWsCS);
\tws=g_hFyersWebSocket;conn=g_hFyersWsConnection;session=g_hFyersWsSession;g_hFyersWebSocket=NULL;g_hFyersWsConnection=NULL;g_hFyersWsSession=NULL;LeaveCriticalSection(&g_fyersWsCS);
\tif(ws)WinHttpWebSocketClose(ws,WINHTTP_WEB_SOCKET_SUCCESS_CLOSE_STATUS,NULL,0);LeaveCriticalSection(&g_fyersWsIoCS);
\tif(ws)WinHttpCloseHandle(ws);if(conn)WinHttpCloseHandle(conn);if(session)WinHttpCloseHandle(session);
\tif(g_hFyersConnectedEvent)ResetEvent(g_hFyersConnectedEvent);if(g_hFyersAuthenticatedEvent)ResetEvent(g_hFyersAuthenticatedEvent);
}

static BOOL FyersBuildAuthMessage(std::vector<BYTE>& msg,const CString& hsmKey)
{
\tCStringA hsm(hsmKey);const char* source="CSDK-3.1.8";size_t hl=(size_t)hsm.GetLength(),sl=strlen(source),total=18+hl+sl;if(total>65535)return FALSE;
\tmsg.assign(total,0);uint16_t dl=(uint16_t)(total-2);msg[0]=(BYTE)(dl>>8);msg[1]=(BYTE)dl;msg[2]=1;msg[3]=4;msg[4]=1;msg[5]=(BYTE)(hl>>8);msg[6]=(BYTE)hl;memcpy(msg.data()+7,hsm.GetString(),hl);
\tsize_t o=7+hl;msg[o++]=2;msg[o++]=0;msg[o++]=1;msg[o++]='P';msg[o++]=3;msg[o++]=0;msg[o++]=1;msg[o++]=1;msg[o++]=4;msg[o++]=(BYTE)(sl>>8);msg[o++]=(BYTE)sl;memcpy(msg.data()+o,source,sl);return TRUE;
}
static BOOL FyersBuildModeMessage(std::vector<BYTE>& msg)
{
\tmsg.assign(19,0);msg[2]=12;msg[3]=2;msg[4]=1;msg[5]=0;msg[6]=8;uint64_t bits=(uint64_t)1<<11;for(int i=0;i<8;i++)msg[7+i]=(BYTE)((bits>>(56-i*8))&0xFF);
\tmsg[15]=2;msg[16]=0;msg[17]=1;msg[18]=70;uint16_t dl=(uint16_t)(msg.size()-2);msg[0]=(BYTE)(dl>>8);msg[1]=(BYTE)dl;return TRUE;
}

static HINTERNET FyersConnectWebSocket(void)
{
\tHINTERNET session=NULL,conn=NULL,request=NULL,ws=NULL;CString hsm;g_fyersLastError.Empty();if(!FyersExtractHsmKeyFromAccessToken(hsm))return NULL;
\tsession=WinHttpOpen(L"FYERS Direct AmiBroker Data Plugin/1.0",WINHTTP_ACCESS_TYPE_AUTOMATIC_PROXY,WINHTTP_NO_PROXY_NAME,WINHTTP_NO_PROXY_BYPASS,0);if(!session){FyersSetWinHttpError(_T("WinHttpOpen(WebSocket)"),GetLastError());goto fail;}
\tWinHttpSetTimeouts(session,5000,5000,FYERS_WS_RECEIVE_TIMEOUT_MS,FYERS_WS_RECEIVE_TIMEOUT_MS);
\tconn=WinHttpConnect(session,FYERS_DIRECT_WS_HOST_W,INTERNET_DEFAULT_HTTPS_PORT,0);if(!conn){FyersSetWinHttpError(_T("WinHttpConnect(socket.fyers.in)"),GetLastError());goto fail;}
\trequest=WinHttpOpenRequest(conn,L"GET",FYERS_DIRECT_WS_PATH_W,NULL,WINHTTP_NO_REFERER,WINHTTP_DEFAULT_ACCEPT_TYPES,WINHTTP_FLAG_SECURE);if(!request){FyersSetWinHttpError(_T("WinHttpOpenRequest(WebSocket)"),GetLastError());goto fail;}
\tif(!WinHttpSetOption(request,WINHTTP_OPTION_UPGRADE_TO_WEB_SOCKET,NULL,0)){FyersSetWinHttpError(_T("WinHttpSetOption(UPGRADE_TO_WEB_SOCKET)"),GetLastError());goto fail;}
\t{const wchar_t hdrs[]=L"Origin: https://fyers.in\r\n";if(!WinHttpAddRequestHeaders(request,hdrs,(DWORD)-1L,WINHTTP_ADDREQ_FLAG_ADD|WINHTTP_ADDREQ_FLAG_REPLACE)){FyersSetWinHttpError(_T("WinHttpAddRequestHeaders(WebSocket)"),GetLastError());goto fail;}}
\tif(!WinHttpSendRequest(request,WINHTTP_NO_ADDITIONAL_HEADERS,0,WINHTTP_NO_REQUEST_DATA,0,0,0)){FyersSetWinHttpError(_T("WinHttpSendRequest(WebSocket)"),GetLastError());goto fail;}
\tif(!WinHttpReceiveResponse(request,NULL)){FyersSetWinHttpError(_T("WinHttpReceiveResponse(WebSocket)"),GetLastError());goto fail;}
\t{DWORD status=0,size=sizeof(status);if(!WinHttpQueryHeaders(request,WINHTTP_QUERY_STATUS_CODE|WINHTTP_QUERY_FLAG_NUMBER,WINHTTP_HEADER_NAME_BY_INDEX,&status,&size,WINHTTP_NO_HEADER_INDEX)){FyersSetWinHttpError(_T("WinHttpQueryHeaders(WebSocket)"),GetLastError());goto fail;}if(status!=101){CString m;m.Format(_T("FYERS WebSocket upgrade failed: HTTP %lu (expected 101)."),(unsigned long)status);FyersSetLastError(m);goto fail;}}
\tws=WinHttpWebSocketCompleteUpgrade(request,0);if(!ws){FyersSetWinHttpError(_T("WinHttpWebSocketCompleteUpgrade"),GetLastError());goto fail;}WinHttpCloseHandle(request);request=NULL;
\tEnterCriticalSection(&g_fyersWsCS);g_hFyersWebSocket=ws;g_hFyersWsConnection=conn;g_hFyersWsSession=session;LeaveCriticalSection(&g_fyersWsCS);conn=NULL;session=NULL;
\tif(g_hFyersConnectedEvent)SetEvent(g_hFyersConnectedEvent);if(g_hFyersAuthenticatedEvent)ResetEvent(g_hFyersAuthenticatedEvent);
\tstd::vector<BYTE> a,m;if(!FyersBuildAuthMessage(a,hsm)||!FyersBuildModeMessage(m)||!FyersSendWebSocketBinary(a)||!FyersSendWebSocketBinary(m)){FyersSetLastError(_T("FYERS WebSocket: failed to send HSM authentication/mode message."));}
\treturn ws;
fail:
\tif(request)WinHttpCloseHandle(request);if(conn)WinHttpCloseHandle(conn);if(session)WinHttpCloseHandle(session);return NULL;
}

static void FyersProcessWebSocketControlMessage(const BYTE* data,size_t len)
{
\tif(!data||len<3)return;BYTE type=data[2];
\tif(type==1){if(len<7)return;size_t off=4;BOOL ok=TRUE;off++;uint16_t fl=FyersReadBE16(data,len,off,ok);if(!ok||off+fl>len)return;CStringA r((LPCSTR)(data+off),(int)fl);
\t\tif(r=="K"){g_nStatus=STATUS_CONNECTED;if(g_hFyersAuthenticatedEvent)SetEvent(g_hFyersAuthenticatedEvent);FyersSetLastError(_T("FYERS WebSocket: HSM authentication succeeded."));FyersSubscribeAll();if(g_hAmiBrokerWnd)PostMessage(g_hAmiBrokerWnd,WM_USER_STREAMING_UPDATE,0,0);}
\t\telse{CString m;m.Format(_T("FYERS WebSocket: HSM authentication rejected ('%s')."),(LPCTSTR)CString(r));FyersSetLastError(m);}return;}
\tif(type==4){BOOL ok=TRUE;size_t off=5;uint16_t fl=FyersReadBE16(data,len,off,ok);if(!ok||off+fl>len)return;CStringA r((LPCSTR)(data+off),(int)fl);if(r=="K")FyersSetLastError(_T("FYERS WebSocket: subscription accepted."));else{CString m;m.Format(_T("FYERS WebSocket: subscription rejected ('%s')."),(LPCTSTR)CString(r));FyersSetLastError(m);}return;}
}

static UINT __cdecl FyersWebSocketThreadProc(LPVOID)
{
\tAFX_MANAGE_STATE(AfxGetStaticModuleState());std::vector<BYTE> fragment;ULONGLONG lastHeartbeat=GetTickCount64();
\twhile(InterlockedCompareExchange(&g_bFyersWsShouldStop,0,0)==0)
\t{
\t\tif(InterlockedExchange(&g_bFyersForceReconnect,0)!=0){FyersCloseWebSocketOnOwnerThread();fragment.clear();}
\t\tHINTERNET ws=NULL;EnterCriticalSection(&g_fyersWsCS);ws=g_hFyersWebSocket;LeaveCriticalSection(&g_fyersWsCS);
\t\tif(!ws){if(!FyersCredentialsReady()){g_nStatus=STATUS_WAIT;Sleep(FYERS_RECONNECT_DELAY_MS);continue;}g_nStatus=STATUS_WAIT;ws=FyersConnectWebSocket();if(!ws){g_nStatus=STATUS_DISCONNECTED;Sleep(FYERS_RECONNECT_DELAY_MS);continue;}lastHeartbeat=GetTickCount64();}
\t\tBYTE buffer[65536];DWORD received=0;WINHTTP_WEB_SOCKET_BUFFER_TYPE bt=WINHTTP_WEB_SOCKET_BINARY_MESSAGE_BUFFER_TYPE;DWORD rc=WinHttpWebSocketReceive(ws,buffer,sizeof(buffer),&received,&bt);
\t\tif(rc==NO_ERROR)
\t\t{
\t\t\tif(bt==WINHTTP_WEB_SOCKET_BINARY_FRAGMENT_BUFFER_TYPE){fragment.insert(fragment.end(),buffer,buffer+received);continue;}
\t\t\tif(bt==WINHTTP_WEB_SOCKET_BINARY_MESSAGE_BUFFER_TYPE){const BYTE* p=buffer;size_t n=received;if(!fragment.empty()){fragment.insert(fragment.end(),buffer,buffer+received);p=fragment.data();n=fragment.size();}
\t\t\t\tif(n>=3&&p[2]==6)FyersProcessWebSocketPayload(p,n);else FyersProcessWebSocketControlMessage(p,n);fragment.clear();lastHeartbeat=GetTickCount64();continue;}
\t\t\tif(bt==WINHTTP_WEB_SOCKET_CLOSE_BUFFER_TYPE){FyersCloseWebSocketOnOwnerThread();g_nStatus=STATUS_DISCONNECTED;continue;}
\t\t}
\t\telse if(rc==ERROR_WINHTTP_TIMEOUT){ULONGLONG now=GetTickCount64();if(now-lastHeartbeat>=(ULONGLONG)FYERS_WS_HEARTBEAT_SEC*1000ULL){BYTE ping[]={0,1,11};FyersSendWebSocketBinary(ping,(DWORD)sizeof(ping));lastHeartbeat=now;}continue;}
\t\telse{FyersSetWinHttpError(_T("WinHttpWebSocketReceive"),rc);FyersCloseWebSocketOnOwnerThread();g_nStatus=STATUS_DISCONNECTED;}
\t}
\tFyersCloseWebSocketOnOwnerThread();g_nStatus=STATUS_SHUTDOWN;return 0;
}

static void FyersQueueHistoryJob(const CString& ticker,
                                 int periodicity,
                                 time_t startTs,
                                 time_t endTs,
                                 BOOL initialLoad)
{
	if (ticker.IsEmpty() || startTs <= 0 || endTs < startTs ||
	    !g_fyersHistoryQueueCSInitialized)
		return;

	FyersSymbolState* state = FyersGetOrCreateState(ticker);
	if (!state)
		return;

	BOOL shouldQueue = FALSE;
	EnterCriticalSection(&g_fyersStateCS);
	if (periodicity == FYERS_PERIOD_5SEC)
	{
		if (!state->fiveSecFetchInProgress)
		{
			state->fiveSecFetchInProgress = TRUE;
			shouldQueue = TRUE;
		}
	}
	else if (periodicity == FYERS_PERIOD_DAILY)
	{
		if (!state->dailyFetchInProgress)
		{
			state->dailyFetchInProgress = TRUE;
			shouldQueue = TRUE;
		}
	}
	LeaveCriticalSection(&g_fyersStateCS);

	if (!shouldQueue)
		return;

	FyersHistoryJob job;
	job.ticker = ticker;
	job.periodicity = periodicity;
	job.startTs = startTs;
	job.endTs = endTs;
	job.initialLoad = initialLoad;

	EnterCriticalSection(&g_fyersHistoryQueueCS);
	g_fyersHistoryQueue.AddTail(job);
	LeaveCriticalSection(&g_fyersHistoryQueueCS);

	if (g_hFyersHistoryEvent)
		SetEvent(g_hFyersHistoryEvent);
}

static void FyersQueueStartupHistory(FyersSymbolState* state, int periodicity)
{
	if (!state)
		return;

	time_t now = time(NULL);

	if (periodicity == FYERS_PERIOD_5SEC)
	{
		time_t endTs = ((now / 5) * 5) - 5;
		if (endTs <= 0)
			return;

		time_t startTs = endTs - (time_t)FYERS_5S_INITIAL_LOOKBACK_DAYS * 86400;
		FyersQueueHistoryJob(state->amiTicker, FYERS_PERIOD_5SEC,
		                     startTs, endTs, TRUE);
	}
	else if (periodicity == FYERS_PERIOD_DAILY)
	{
		time_t startTs = now - (time_t)FYERS_DAILY_LOOKBACK_DAYS * 86400;
		FyersQueueHistoryJob(state->amiTicker, FYERS_PERIOD_DAILY,
		                     startTs, now, TRUE);
	}
}

static UINT __cdecl FyersHistoryWorkerProc(LPVOID)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	while (InterlockedCompareExchange(&g_bFyersHistoryShouldStop, 0, 0) == 0)
	{
		if (g_hFyersHistoryEvent)
			WaitForSingleObject(g_hFyersHistoryEvent, 1000);

		for (;;)
		{
			FyersHistoryJob job;
			BOOL haveJob = FALSE;

			EnterCriticalSection(&g_fyersHistoryQueueCS);
			if (!g_fyersHistoryQueue.IsEmpty())
			{
				job = g_fyersHistoryQueue.GetHead();
				g_fyersHistoryQueue.RemoveHead();
				haveJob = TRUE;
			}
			LeaveCriticalSection(&g_fyersHistoryQueueCS);

			if (!haveJob)
				break;

			FyersSymbolState* state = FyersGetOrCreateState(job.ticker);
			if (!state)
				continue;

			std::vector<FyersCandle> all;

			if (job.periodicity == FYERS_PERIOD_DAILY)
			{
				time_t chunkStart = job.startTs;
				while (chunkStart <= job.endTs)
				{
					CTime startT(chunkStart);
					CTime maxEndT = startT + CTimeSpan(365, 0, 0, 0);
					time_t chunkEnd = (time_t)maxEndT.GetTime();
					if (chunkEnd > job.endTs)
						chunkEnd = job.endTs;

					std::vector<FyersCandle> part;
					if (FyersFetchCandles(state->fyersSymbol, FYERS_PERIOD_DAILY,
					                      chunkStart, chunkEnd, part))
					{
						all.insert(all.end(), part.begin(), part.end());
					}

					if (chunkEnd >= job.endTs)
						break;

					chunkStart = chunkEnd + 86400;
				}
			}
			else
			{
				FyersFetchCandles(state->fyersSymbol, FYERS_PERIOD_5SEC,
				                  job.startTs, job.endTs, all);
			}

			if (!all.empty())
			{
				FyersMergeCandles(state, all, job.periodicity);
			}

			ULONGLONG nowTick = GetTickCount64();
			EnterCriticalSection(&g_fyersStateCS);
			if (job.periodicity == FYERS_PERIOD_5SEC)
			{
				state->fiveSecFetchInProgress = FALSE;
				state->lastFiveSecFetchTick = nowTick;
			}
			else
			{
				state->dailyFetchInProgress = FALSE;
				state->lastDailyFetchTick = nowTick;
			}
			LeaveCriticalSection(&g_fyersStateCS);

			CString log;
			log.Format(_T("FYERS direct history: %s %s -> %s, received=%d"),
				(LPCTSTR)job.ticker,
				job.periodicity == FYERS_PERIOD_5SEC ? _T("5S") : _T("D"),
				job.initialLoad ? _T("initial") : _T("gap"),
				(int)all.size());
			OutputDebugString(log);

			if (g_hAmiBrokerWnd)
				PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
		}
	}

	return 0;
}

static BOOL FyersSeedStateFromAmi(FyersSymbolState* state,
                                  int nLastValid,
                                  struct Quotation* pQuotes)
{
	if (!state || state->seededFromAmi || nLastValid < 0 || !pQuotes)
		return FALSE;

	BOOL has5sEvidence = FALSE;
	for (int i = nLastValid; i >= 0 && i >= nLastValid - 1000; i--)
	{
		if (!FyersIsEodQuotation(pQuotes[i]) &&
		    pQuotes[i].DateTime.PackDate.Second != 0)
		{
			has5sEvidence = TRUE;
			break;
		}
	}

	time_t currentBarStart = (time(NULL) / 5) * 5;

	EnterCriticalSection(&g_fyersStateCS);

	for (int i = 0; i <= nLastValid; i++)
	{
		if (FyersIsEodQuotation(pQuotes[i]))
		{
			FyersUpsertQuotation(state->dailyBars, pQuotes[i], FYERS_MAX_DAILY_BARS);
			continue;
		}

		if (has5sEvidence)
		{
			time_t ts = FyersQuotationTimestamp(pQuotes[i]);
			if (ts > 0 && ts < currentBarStart)
			{
				FyersUpsertQuotation(state->fiveSecBars, pQuotes[i], FYERS_MAX_5S_BARS);
			}
			else if (ts == currentBarStart)
			{
				state->currentBar = pQuotes[i];
				state->currentBarStart = currentBarStart;
				state->currentBarStarted = TRUE;
			}
		}
	}

	if (state->fiveSecBars.GetCount() > 0)
		state->lastCompleted5s =
			FyersQuotationTimestamp(state->fiveSecBars[state->fiveSecBars.GetCount() - 1]);

	state->seededFromAmi = TRUE;

	LeaveCriticalSection(&g_fyersStateCS);

	return TRUE;
}

static void FyersMaybeQueueStartupGap(FyersSymbolState* state)
{
	if (!state)
		return;

	BOOL shouldCheck = FALSE;
	EnterCriticalSection(&g_fyersStateCS);
	if (!state->startupGapChecked)
	{
		state->startupGapChecked = TRUE;
		shouldCheck = TRUE;
	}
	LeaveCriticalSection(&g_fyersStateCS);

	if (!shouldCheck)
		return;

	time_t now = time(NULL);
	time_t closed = ((now / 5) * 5) - 5;

	EnterCriticalSection(&g_fyersStateCS);
	time_t last = state->lastCompleted5s;
	if (state->currentBarStarted && state->currentBarStart < closed)
		last = max(last, state->currentBarStart);
	LeaveCriticalSection(&g_fyersStateCS);

	if (last > 0 && closed >= last + 5 &&
	    (closed - last) <= (time_t)FYERS_5S_FALLBACK_LOOKBACK_DAYS * 86400)
	{
		FyersQueueHistoryJob(state->amiTicker, FYERS_PERIOD_5SEC,
		                     last + 5, closed, FALSE);
	}
}

static int FyersCopyDataForPeriod(FyersSymbolState* state,
                                  int periodicity,
                                  int nSize,
                                  struct Quotation* pQuotes)
{
	if (!state || nSize <= 0 || !pQuotes)
		return 0;

	// Daily-only request.
	if (periodicity == FYERS_PERIOD_DAILY)
	{
		EnterCriticalSection(&g_fyersStateCS);
		int count = (int)state->dailyBars.GetCount();
		int take = min(count, nSize);
		int start = count - take;
		if (take > 0)
			memcpy(pQuotes, state->dailyBars.GetData() + start,
			       take * sizeof(Quotation));
		LeaveCriticalSection(&g_fyersStateCS);
		return take;
	}

	// 5-second request: combine older EOD bars with 5S data so AmiBroker can
	// run with "Allow mixed EOD/intraday data" enabled.
	EnterCriticalSection(&g_fyersStateCS);

	int dailyCount = (int)state->dailyBars.GetCount();
	int secCount = (int)state->fiveSecBars.GetCount();

	int dailyToUse = dailyCount;
	if (secCount > 0 && dailyCount > 0)
	{
		AmiDate first5s = state->fiveSecBars[0].DateTime;
		dailyToUse = 0;

		for (int i = 0; i < dailyCount; i++)
		{
			if (FyersCompareYmd(state->dailyBars[i].DateTime, first5s) < 0)
				dailyToUse = i + 1;
			else
				break;
		}
	}

	int total = dailyToUse + secCount;
	int startOffset = max(0, total - nSize);
	int out = 0;

	for (int i = dailyToUse > 0 ? 0 : 0; i < dailyToUse && out < nSize; i++)
	{
		if (i >= startOffset)
			pQuotes[out++] = state->dailyBars[i];
	}

	for (int i = 0; i < secCount && out < nSize; i++)
	{
		int combinedIndex = dailyToUse + i;
		if (combinedIndex >= startOffset)
			pQuotes[out++] = state->fiveSecBars[i];
	}

	// Overlay the currently forming 5-second bar. It is not inserted into
	// the completed-history cache until the next bucket arrives.
	if (state->currentBarStarted && out > 0)
	{
		BOOL replaced = FALSE;
		DATE_TIME_INT key = state->currentBar.DateTime.Date;

		for (int i = out - 1; i >= 0; i--)
		{
			if (pQuotes[i].DateTime.Date == key)
			{
				pQuotes[i] = state->currentBar;
				replaced = TRUE;
				break;
			}
			if (pQuotes[i].DateTime.Date < key)
				break;
		}

		if (!replaced && out < nSize)
			pQuotes[out++] = state->currentBar;
	}

	LeaveCriticalSection(&g_fyersStateCS);
	return out;
}

int FyersDirectGetQuotesEx(LPCTSTR pszTicker,
                                   int nPeriodicity,
                                   int nLastValid,
                                   int nSize,
                                   struct Quotation* pQuotes)
{
	if (!FyersCredentialsReady() ||
	    g_nStatus == STATUS_SHUTDOWN ||
	    pQuotes == NULL ||
	    nSize <= 0)
		return (nLastValid >= 0 ? nLastValid + 1 : 0);

	if (nPeriodicity != FYERS_PERIOD_5SEC &&
	    nPeriodicity != FYERS_PERIOD_DAILY)
	{
		// The database base interval for this build is 5 seconds. AmiBroker
		// compresses 5S data into 10S/15S/30S/1m/etc. itself.
		return (nLastValid >= 0 ? nLastValid + 1 : 0);
	}

	FyersSymbolState* state = FyersGetOrCreateState(pszTicker);
	if (!state)
		return (nLastValid >= 0 ? nLastValid + 1 : 0);

	FyersSeedStateFromAmi(state, nLastValid, pQuotes);
	FyersEnsureSubscribed(pszTicker);

	int cachedCount = 0;
	EnterCriticalSection(&g_fyersStateCS);
	if (nPeriodicity == FYERS_PERIOD_5SEC)
		cachedCount = (int)state->fiveSecBars.GetCount();
	else
		cachedCount = (int)state->dailyBars.GetCount();
	LeaveCriticalSection(&g_fyersStateCS);

	if (cachedCount == 0)
	{
		FyersQueueStartupHistory(state, nPeriodicity);
	}
	else if (nPeriodicity == FYERS_PERIOD_5SEC)
	{
		FyersMaybeQueueStartupGap(state);
	}
	else
	{
		ULONGLONG nowTick = GetTickCount64();
		BOOL stale = FALSE;
		EnterCriticalSection(&g_fyersStateCS);
		stale = (state->lastDailyFetchTick == 0 ||
		         (nowTick - state->lastDailyFetchTick) > FYERS_DAILY_REFRESH_MS);
		BOOL inProgress = state->dailyFetchInProgress;
		LeaveCriticalSection(&g_fyersStateCS);

		if (stale && !inProgress)
		{
			// Refresh today's date + recent range, without reloading all 2 years.
			time_t now = time(NULL);
			time_t start = now - 10 * 86400;
			FyersQueueHistoryJob(state->amiTicker, FYERS_PERIOD_DAILY,
			                     start, now, FALSE);
		}
	}

	int nQty = FyersCopyDataForPeriod(state, nPeriodicity, nSize, pQuotes);
	if (nQty == 0)
		return (nLastValid >= 0 ? nLastValid + 1 : 0);

	return nQty;
}

struct RecentInfo* FyersDirectGetRecentInfo(LPCTSTR pszTicker)
{
	if (!FyersCredentialsReady() || !g_fyersRecentInfoCSInitialized)
		return NULL;

	FyersSymbolState* state = FyersGetOrCreateState(pszTicker);
	if (!state)
		return NULL;

	FyersEnsureSubscribed(pszTicker);
	return FyersGetOrCreateRecentInfo(pszTicker);
}

BOOL FyersTestWebSocket(void)
{
	if (!FyersCredentialsReady())
		return FALSE;

	if (g_hFyersConnectedEvent)
		ResetEvent(g_hFyersConnectedEvent);

	InterlockedExchange(&g_bFyersForceReconnect, 1);

	if (g_hFyersConnectedEvent &&
	    WaitForSingleObject(g_hFyersConnectedEvent, 5000) == WAIT_OBJECT_0)
		return TRUE;

	return FALSE;
}

static void FyersRequestReconnect(void)
{
	InterlockedExchange(&g_bFyersForceReconnect, 1);
}

BOOL FyersDirectInit(void)
{
	if (!g_fyersStateCSInitialized)
	{
		InitializeCriticalSection(&g_fyersStateCS);
		g_fyersStateCSInitialized = TRUE;
		g_fyersStates.InitHashTable(503);
	}

	if (!g_fyersRecentInfoCSInitialized)
	{
		InitializeCriticalSection(&g_fyersRecentInfoCS);
		g_fyersRecentInfoCSInitialized = TRUE;
		g_fyersRecentInfo.InitHashTable(503);
	}

	if (!g_fyersHistoryQueueCSInitialized)
	{
		InitializeCriticalSection(&g_fyersHistoryQueueCS);
		g_fyersHistoryQueueCSInitialized = TRUE;
	}

	if (!g_fyersWsCSInitialized)
	{
		InitializeCriticalSection(&g_fyersWsCS);
		g_fyersWsCSInitialized = TRUE;
	}

	if (!g_fyersWsIoCSInitialized)
	{
		InitializeCriticalSection(&g_fyersWsIoCS);
		g_fyersWsIoCSInitialized = TRUE;
	}

	if (!g_hFyersHistoryEvent)
		g_hFyersHistoryEvent = CreateEvent(NULL, FALSE, FALSE, NULL);

	if (!g_hFyersConnectedEvent)
		g_hFyersConnectedEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
	if (!g_hFyersAuthenticatedEvent)
		g_hFyersAuthenticatedEvent = CreateEvent(NULL, TRUE, FALSE, NULL);

	if (!FyersLoadSettings())
		FyersLoadLegacyAccessTokenFile();

	if (!g_fyersAppId.IsEmpty())
		g_fyersAppId = FyersNormalizeAppId(g_fyersAppId);
	g_fyersAccessToken = FyersNormalizeAccessToken(g_fyersAccessToken);
	g_oServer = g_fyersAppId;
	g_oApiKey = g_fyersAccessToken;

	InterlockedExchange(&g_bFyersHistoryShouldStop, 0);
	InterlockedExchange(&g_bFyersWsShouldStop, 0);
	InterlockedExchange(&g_bFyersForceReconnect, 0);

	if (!g_pFyersHistoryThread)
	{
		g_pFyersHistoryThread = AfxBeginThread(
			FyersHistoryWorkerProc, NULL,
			THREAD_PRIORITY_BELOW_NORMAL, 0, CREATE_SUSPENDED, NULL);
		if (g_pFyersHistoryThread)
		{
			g_pFyersHistoryThread->m_bAutoDelete = FALSE;
			g_pFyersHistoryThread->ResumeThread();
		}
	}

	if (!g_pFyersWsThread)
	{
		g_pFyersWsThread = AfxBeginThread(
			FyersWebSocketThreadProc, NULL,
			THREAD_PRIORITY_NORMAL, 0, CREATE_SUSPENDED, NULL);
		if (g_pFyersWsThread)
		{
			g_pFyersWsThread->m_bAutoDelete = FALSE;
			g_pFyersWsThread->ResumeThread();
		}
	}

	g_nStatus = FyersCredentialsReady() ? STATUS_WAIT : STATUS_DISCONNECTED;

	return 1;
}

void FyersDirectRelease(void)
{
	InterlockedExchange(&g_bFyersWsShouldStop, 1);
	InterlockedExchange(&g_bFyersHistoryShouldStop, 1);

	if (g_hFyersHistoryEvent)
		SetEvent(g_hFyersHistoryEvent);

	if (g_pFyersWsThread)
	{
		WaitForSingleObject(g_pFyersWsThread->m_hThread, 10000);
		delete g_pFyersWsThread;
		g_pFyersWsThread = NULL;
	}

	if (g_pFyersHistoryThread)
	{
		WaitForSingleObject(g_pFyersHistoryThread->m_hThread, 10000);
		delete g_pFyersHistoryThread;
		g_pFyersHistoryThread = NULL;
	}

	if (g_hFyersAuthenticatedEvent)
	{
		CloseHandle(g_hFyersAuthenticatedEvent);
		g_hFyersAuthenticatedEvent = NULL;
	}
	if (g_hFyersConnectedEvent)
	{
		CloseHandle(g_hFyersConnectedEvent);
		g_hFyersConnectedEvent = NULL;
	}
	if (g_hFyersHistoryEvent)
	{
		CloseHandle(g_hFyersHistoryEvent);
		g_hFyersHistoryEvent = NULL;
	}

	if (g_fyersHistoryQueueCSInitialized)
	{
		EnterCriticalSection(&g_fyersHistoryQueueCS);
		g_fyersHistoryQueue.RemoveAll();
		LeaveCriticalSection(&g_fyersHistoryQueueCS);
		DeleteCriticalSection(&g_fyersHistoryQueueCS);
		g_fyersHistoryQueueCSInitialized = FALSE;
	}

	if (g_fyersWsIoCSInitialized)
	{
		DeleteCriticalSection(&g_fyersWsIoCS);
		g_fyersWsIoCSInitialized = FALSE;
	}

	if (g_fyersWsCSInitialized)
	{
		DeleteCriticalSection(&g_fyersWsCS);
		g_fyersWsCSInitialized = FALSE;
	}

	if (g_fyersRecentInfoCSInitialized)
	{
		EnterCriticalSection(&g_fyersRecentInfoCS);
		POSITION pos = g_fyersRecentInfo.GetStartPosition();
		while (pos)
		{
			CString key;
			struct RecentInfo* pInfo = NULL;
			g_fyersRecentInfo.GetNextAssoc(pos, key, pInfo);
			delete pInfo;
		}
		g_fyersRecentInfo.RemoveAll();
		LeaveCriticalSection(&g_fyersRecentInfoCS);
		DeleteCriticalSection(&g_fyersRecentInfoCS);
		g_fyersRecentInfoCSInitialized = FALSE;
	}

	if (g_fyersStateCSInitialized)
	{
		FyersClearStates();
		DeleteCriticalSection(&g_fyersStateCS);
		g_fyersStateCSInitialized = FALSE;
	}

	g_nStatus = STATUS_SHUTDOWN;
}

BOOL FyersDirectNotify(struct PluginNotification* pn)
{
	if (!pn)
		return 0;

	if (pn->nReason & REASON_DATABASE_LOADED)
	{
		g_hAmiBrokerWnd = pn->hMainWnd;
		g_nStatus = FyersCredentialsReady() ? STATUS_WAIT : STATUS_DISCONNECTED;

		// Keep state objects alive while the background WS/history threads run.
		// AmiBroker may switch databases while those threads are still inside
		// a state lookup; deleting objects here would create a use-after-free.
		// Reset the objects instead. Release() performs the final destruction
		// only after both worker threads have stopped.
		FyersResetStatesForDatabase();
	}
	
	if (pn->nReason & REASON_DATABASE_UNLOADED)
	{
		g_hAmiBrokerWnd = NULL;
		FyersResetStatesForDatabase();
	}

	if (pn->nReason & REASON_SETTINGS_CHANGE)
	{
		FyersRequestReconnect();
	}

	return 1;
}

BOOL FyersDirectReconfigure(CString appId, CString accessToken)
{
	appId.Trim();
	accessToken.Trim();

	if (appId.IsEmpty() || accessToken.IsEmpty())
		return FALSE;

	g_fyersAppId = FyersNormalizeAppId(appId);
	g_fyersAccessToken = accessToken;
	g_oServer = g_fyersAppId;
	g_oApiKey = g_fyersAccessToken;

	if (!FyersSaveSettings())
		return FALSE;

	FyersRequestReconnect();
	return TRUE;
}

PLUGINAPI int GetPluginInfo(struct PluginInfo* pInfo){AFX_MANAGE_STATE(AfxGetStaticModuleState());if(!pInfo)return FALSE;*pInfo=oPluginInfo;return TRUE;}
PLUGINAPI int Init(void){AFX_MANAGE_STATE(AfxGetStaticModuleState());return FyersDirectInit()?1:0;}
PLUGINAPI int Release(void){AFX_MANAGE_STATE(AfxGetStaticModuleState());FyersDirectRelease();return 1;}
PLUGINAPI int Configure(LPCTSTR p,struct InfoSite* s){AFX_MANAGE_STATE(AfxGetStaticModuleState());UNREFERENCED_PARAMETER(p);COpenAlgoConfigDlg d;d.m_pSite=s;if(d.DoModal()==IDOK&&g_hAmiBrokerWnd)::PostMessage(g_hAmiBrokerWnd,WM_USER_STREAMING_UPDATE,0,0);return 1;}
PLUGINAPI AmiVar GetExtraData(LPCTSTR a,LPCTSTR b,int c,int d,void*(*e)(unsigned int)){UNREFERENCED_PARAMETER(a);UNREFERENCED_PARAMETER(b);UNREFERENCED_PARAMETER(c);UNREFERENCED_PARAMETER(d);UNREFERENCED_PARAMETER(e);AmiVar v;v.type=VAR_NONE;v.val=0;return v;}
PLUGINAPI int SetTimeBase(int n)
{
    // FYERS Direct supplies native 5-second intraday bars. AmiBroker then
    // compresses those base bars into higher intraday chart periods.
    // EOD is also supported for databases configured with a daily base.
    return (n == 5 || n == 86400) ? 1 : 0;
}
PLUGINAPI int GetSymbolLimit(void){return 5000;}
PLUGINAPI int GetPluginStatus(struct PluginStatus* s){AFX_MANAGE_STATE(AfxGetStaticModuleState());if(!s)return 0;s->nStructSize=sizeof(*s);if(g_nStatus==STATUS_CONNECTED){s->nStatusCode=0;strcpy_s(s->szShortMessage,sizeof(s->szShortMessage),"OK");strcpy_s(s->szLongMessage,sizeof(s->szLongMessage),"FYERS Direct: Connected");s->clrStatusColor=RGB(0,255,0);}else if(g_nStatus==STATUS_WAIT){s->nStatusCode=0x10000000;strcpy_s(s->szShortMessage,sizeof(s->szShortMessage),"WAIT");strcpy_s(s->szLongMessage,sizeof(s->szLongMessage),"FYERS Direct: Waiting for market-data connection");s->clrStatusColor=RGB(255,255,0);}else if(g_nStatus==STATUS_SHUTDOWN){s->nStatusCode=0x30000000;strcpy_s(s->szShortMessage,sizeof(s->szShortMessage),"OFF");strcpy_s(s->szLongMessage,sizeof(s->szLongMessage),"FYERS Direct: Offline");s->clrStatusColor=RGB(192,0,192);}else{s->nStatusCode=0x20000000;strcpy_s(s->szShortMessage,sizeof(s->szShortMessage),"ERR");strcpy_s(s->szLongMessage,sizeof(s->szLongMessage),"FYERS Direct: Connection failed; automatic retry is active");s->clrStatusColor=RGB(255,0,0);}return 1;}
PLUGINAPI int GetStatus(struct PluginStatus* s){return GetPluginStatus(s);}
PLUGINAPI int SetDatabasePath(LPCTSTR p){UNREFERENCED_PARAMETER(p);return 1;}
PLUGINAPI struct RecentInfo* GetRecentInfo(LPCTSTR t){AFX_MANAGE_STATE(AfxGetStaticModuleState());return FyersDirectGetRecentInfo(t);}
PLUGINAPI BOOL IsBackfillComplete(LPCTSTR t){AFX_MANAGE_STATE(AfxGetStaticModuleState());if(!t)return FALSE;FyersSymbolState* s=FyersGetOrCreateState(t);if(!s)return FALSE;BOOL ready,busy;EnterCriticalSection(&g_fyersStateCS);ready=s->fiveSecBars.GetCount()>0||s->dailyBars.GetCount()>0;busy=s->fiveSecFetchInProgress||s->dailyFetchInProgress;LeaveCriticalSection(&g_fyersStateCS);return ready||!busy;}
PLUGINAPI int GetQuotesEx(LPCTSTR t,int p,int last,int size,Quotation* q,GQEContext* c){AFX_MANAGE_STATE(AfxGetStaticModuleState());UNREFERENCED_PARAMETER(c);return FyersDirectGetQuotesEx(t,p,last,size,q);}
PLUGINAPI int GetQuotes(LPCTSTR t,int p,int last,int size,QuotationFormat4* q4){AFX_MANAGE_STATE(AfxGetStaticModuleState());if(!q4||size<=0)return last>=0?last+1:0;Quotation* q=(Quotation*)malloc((size_t)size*sizeof(Quotation));if(!q)return last>=0?last+1:0;int seed=min(size,max(0,last+1));for(int i=0;i<seed;++i)ConvertFormat4Quote(&q4[i],&q[i]);int n=FyersDirectGetQuotesEx(t,p,last,size,q);for(int i=0;i<n&&i<size;++i)ConvertFormat5Quote(&q[i],&q4[i]);free(q);return n;}
PLUGINAPI int Notify(struct PluginNotification* pn){AFX_MANAGE_STATE(AfxGetStaticModuleState());if(!pn)return 0;return FyersDirectNotify(pn)?1:0;}
