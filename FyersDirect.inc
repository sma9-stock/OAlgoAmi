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
//   WS    wss://api.fyers.in/socket/v2/data/
//   Auth  Authorization: <app_id>:<access_token>
// -----------------------------------------------------------------------------

#include <winhttp.h>
#include <wincrypt.h>
#include <vector>
#include <algorithm>
#include <cmath>
#pragma comment(lib, "winhttp.lib")
#pragma comment(lib, "crypt32.lib")

#define FYERS_DIRECT_HOST              _T("api-t1.fyers.in")
#define FYERS_DIRECT_WS_HOST           _T("api.fyers.in")
#define FYERS_DIRECT_WS_PATH           _T("/socket/v2/data/")
#define FYERS_DIRECT_HISTORY_PATH      _T("/data/history")
#define FYERS_DIRECT_QUOTES_PATH       _T("/data/quotes")

#define FYERS_PERIOD_5SEC              5
#define FYERS_PERIOD_DAILY             86400

// FYERS exposes seconds history only for the most recent 30 trading days.
// We deliberately ask a slightly wider calendar range for the initial load;
// the provider will cap the result to the available seconds-history window.
#define FYERS_5S_INITIAL_LOOKBACK_DAYS 45
#define FYERS_5S_FALLBACK_LOOKBACK_DAYS 30
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

struct FyersSymbolState
{
	CString amiTicker;
	CString fyersSymbol;

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

static CString FyersTrim(CString value)
{
	value.Trim();
	return value;
}

static CString FyersNormalizeAppId(CString appId)
{
	appId.Trim();
	if (appId.IsEmpty())
		return appId;

	if (appId.Right(4).CompareNoCase(_T("-100")) != 0)
		appId += _T("-100");

	return appId;
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

static BOOL FyersHttpGet(const CString& pathAndQuery, CStringA& response, DWORD& statusCode)
{
	response.Empty();
	statusCode = 0;

	if (g_fyersAppId.IsEmpty() || g_fyersAccessToken.IsEmpty())
		return FALSE;

	HINTERNET hSession = NULL;
	HINTERNET hConnect = NULL;
	HINTERNET hRequest = NULL;
	BOOL ok = FALSE;

	try
	{
		hSession = WinHttpOpen(
			L"OpenAlgo AmiBroker / FYERS Direct",
			WINHTTP_ACCESS_TYPE_AUTOMATIC_PROXY,
			WINHTTP_NO_PROXY_NAME,
			WINHTTP_NO_PROXY_BYPASS,
			0);

		if (!hSession)
			goto cleanup;

		WinHttpSetTimeouts(hSession, 5000, 5000, 10000, 10000);

		hConnect = WinHttpConnect(hSession, FYERS_DIRECT_HOST,
		                          INTERNET_DEFAULT_HTTPS_PORT, 0);
		if (!hConnect)
			goto cleanup;

		hRequest = WinHttpOpenRequest(
			hConnect,
			L"GET",
			pathAndQuery,
			NULL,
			WINHTTP_NO_REFERER,
			WINHTTP_DEFAULT_ACCEPT_TYPES,
			WINHTTP_FLAG_SECURE);
		if (!hRequest)
			goto cleanup;

		CString auth;
		auth.Format(_T("Authorization: %s:%s\r\n"),
			(LPCTSTR)FyersNormalizeAppId(g_fyersAppId),
			(LPCTSTR)g_fyersAccessToken);

		if (!WinHttpAddRequestHeaders(hRequest, auth, (DWORD)-1L,
		                              WINHTTP_ADDREQ_FLAG_ADD | WINHTTP_ADDREQ_FLAG_REPLACE))
			goto cleanup;

		if (!WinHttpSendRequest(hRequest,
		                        WINHTTP_NO_ADDITIONAL_HEADERS, 0,
		                        WINHTTP_NO_REQUEST_DATA, 0, 0, 0))
			goto cleanup;

		if (!WinHttpReceiveResponse(hRequest, NULL))
			goto cleanup;

		DWORD size = sizeof(statusCode);
		WinHttpQueryHeaders(
			hRequest,
			WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER,
			WINHTTP_HEADER_NAME_BY_INDEX,
			&statusCode,
			&size,
			WINHTTP_NO_HEADER_INDEX);

		for (;;)
		{
			DWORD available = 0;
			if (!WinHttpQueryDataAvailable(hRequest, &available))
				goto cleanup;
			if (available == 0)
				break;

			std::vector<char> buf(available + 1, 0);
			DWORD read = 0;
			if (!WinHttpReadData(hRequest, buf.data(), available, &read))
				goto cleanup;
			if (read)
				response.Append(buf.data(), (int)read);
		}

		ok = (statusCode >= 200 && statusCode < 300);

cleanup:
		if (hRequest) WinHttpCloseHandle(hRequest);
		if (hConnect) WinHttpCloseHandle(hConnect);
		if (hSession) WinHttpCloseHandle(hSession);
		return ok;
	}
	catch (...)
	{
		if (hRequest) WinHttpCloseHandle(hRequest);
		if (hConnect) WinHttpCloseHandle(hConnect);
		if (hSession) WinHttpCloseHandle(hSession);
		return FALSE;
	}
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

static BOOL FyersTestRestConnection(void)
{
	CStringA response;
	DWORD status = 0;

	CString symbol = FyersUrlEncode(_T("NSE:NIFTY50-INDEX"));
	CString path;
	path.Format(_T("%s?symbols=%s"),
	            FYERS_DIRECT_QUOTES_PATH, (LPCTSTR)symbol);

	return FyersHttpGet(path, response, status);
}

static BOOL FyersSendWebSocketUtf8(const CStringA& message)
{
	BOOL result = FALSE;

	if (!g_fyersWsIoCSInitialized || !g_fyersWsCSInitialized)
		return FALSE;

	EnterCriticalSection(&g_fyersWsIoCS);

	HINTERNET ws = NULL;
	EnterCriticalSection(&g_fyersWsCS);
	ws = g_hFyersWebSocket;
	LeaveCriticalSection(&g_fyersWsCS);

	if (ws)
	{
		DWORD rc = WinHttpWebSocketSend(
			ws,
			WINHTTP_WEB_SOCKET_UTF8_MESSAGE_BUFFER_TYPE,
			(PVOID)message.GetString(),
			(DWORD)message.GetLength());
		result = (rc == NO_ERROR);
	}

	LeaveCriticalSection(&g_fyersWsIoCS);
	return result;
}

static BOOL FyersSendSubscribeSymbol(const CString& ticker)
{
	FyersSymbolState* state = FyersGetOrCreateState(ticker);
	if (!state)
		return FALSE;

	CStringA fyersSymbolA(state->fyersSymbol);
	CStringA json;
	json.Format("{\"symbol\":[\"%s\"],\"type\":\"symbolUpdate\"}",
	            (LPCSTR)fyersSymbolA);

	return FyersSendWebSocketUtf8(json);
}

static void FyersSubscribeAll(void)
{
	std::vector<CString> symbols;

	EnterCriticalSection(&g_fyersStateCS);
	POSITION pos = g_fyersStates.GetStartPosition();
	while (pos != NULL)
	{
		CString key;
		FyersSymbolState* state = NULL;
		g_fyersStates.GetNextAssoc(pos, key, state);
		if (state && state->subscribed)
			symbols.push_back(state->fyersSymbol);
	}
	LeaveCriticalSection(&g_fyersStateCS);

	for (size_t i = 0; i < symbols.size(); i++)
	{
		CStringA fyersSymbolA(symbols[i]);
		CStringA json;
		json.Format("{\"symbol\":[\"%s\"],\"type\":\"symbolUpdate\"}",
		            (LPCSTR)fyersSymbolA);
		FyersSendWebSocketUtf8(json);
	}
}

static BOOL FyersEnsureSubscribed(LPCTSTR pszTicker)
{
	FyersSymbolState* state = FyersGetOrCreateState(pszTicker);
	if (!state)
		return FALSE;

	BOOL fresh = FALSE;
	EnterCriticalSection(&g_fyersStateCS);
	if (!state->subscribed)
	{
		state->subscribed = TRUE;
		fresh = TRUE;
	}
	LeaveCriticalSection(&g_fyersStateCS);

	if (fresh)
	{
		// If the socket is already up, add this symbol immediately. If it isn't,
		// FyersSubscribeAll() will include it on the next connection.
		BOOL connected = FALSE;
		EnterCriticalSection(&g_fyersWsCS);
		connected = (g_hFyersWebSocket != NULL);
		LeaveCriticalSection(&g_fyersWsCS);

		if (connected)
			FyersSendSubscribeSymbol(pszTicker);
	}

	return TRUE;
}

static BOOL FyersParseTimestampField(const CString& raw, time_t& outTs)
{
	CString s = raw;
	s.Trim();
	if (s.IsEmpty())
		return FALSE;

	__int64 v = _tstoi64(s);
	if (v > 1000000000000LL)
		v /= 1000;

	if (v < 1000000000LL)
		return FALSE;

	outTs = (time_t)v;
	return TRUE;
}

static void FyersHandleMarketMessage(const CString& message)
{
	// FYERS SymbolUpdate messages are JSON objects with fields such as:
	// symbol, ltp, vol_traded_today, last_traded_time, last_traded_qty,
	// open_price, high_price, low_price, prev_close_price, bid_price, ask_price.
	if (message.Find(_T("\"symbol\"")) < 0 ||
	    message.Find(_T("\"ltp\"")) < 0)
		return;

	CString ticker = ExtractJsonStringValue(message, _T("symbol"));
	if (ticker.IsEmpty())
		return;

	float ltp        = ExtractJsonFloatValue(message, _T("ltp"));
	float open       = ExtractJsonFloatValue(message, _T("open_price"));
	float high       = ExtractJsonFloatValue(message, _T("high_price"));
	float low        = ExtractJsonFloatValue(message, _T("low_price"));
	float prevClose  = ExtractJsonFloatValue(message, _T("prev_close_price"));
	float totalVol   = ExtractJsonFloatValue(message, _T("vol_traded_today"));
	float tradeQty   = ExtractJsonFloatValue(message, _T("last_traded_qty"));
	float oi         = ExtractJsonFloatValue(message, _T("oi"));
	float bid        = ExtractJsonFloatValue(message, _T("bid_price"));
	float ask        = ExtractJsonFloatValue(message, _T("ask_price"));

	CString rawTs = ExtractJsonRawValue(message, _T("last_traded_time"));
	if (rawTs.IsEmpty())
		rawTs = ExtractJsonRawValue(message, _T("timestamp"));

	time_t tradeTs = 0;
	if (!FyersParseTimestampField(rawTs, tradeTs) || tradeTs <= 0)
		tradeTs = time(NULL);

	if (ltp <= 0)
		return;

	FyersSymbolState* state = FyersGetOrCreateState(ticker);
	if (!state)
		return;

	time_t barStart = (tradeTs / FYERS_PERIOD_5SEC) * FYERS_PERIOD_5SEC;
	time_t gapStart = 0;
	time_t gapEnd = 0;
	BOOL isTrade = TRUE;

	EnterCriticalSection(&g_fyersStateCS);

	// Calculate volume from the cumulative FYERS day volume whenever available.
	// This avoids double-counting repeated SymbolUpdate frames.
	float volumeDelta = 0.0f;
	if (totalVol > 0.0f)
	{
		if (state->haveTotalVolume && totalVol >= state->lastTotalVolume)
			volumeDelta = totalVol - state->lastTotalVolume;
		else if (!state->haveTotalVolume)
			volumeDelta = tradeQty;

		state->lastTotalVolume = totalVol;
		state->haveTotalVolume = TRUE;
	}
	else
	{
		volumeDelta = tradeQty;
	}

	BOOL duplicateTrade = FALSE;
	if (state->lastTradeTs == tradeTs && ltp == state->currentBar.Price &&
	    fabs((double)volumeDelta) < 0.0001)
		duplicateTrade = TRUE;

	// Detect a discontinuity in trade-time buckets. If there was a genuine
	// missing interval, REST 5S history will be used to repair it.
	if (state->currentBarStarted && barStart > state->currentBarStart + FYERS_PERIOD_5SEC)
	{
		gapStart = state->currentBarStart + FYERS_PERIOD_5SEC;
		gapEnd = barStart - FYERS_PERIOD_5SEC;
	}

	if (duplicateTrade)
		isTrade = FALSE;

	if (!state->currentBarStarted || barStart != state->currentBarStart)
	{
		if (state->currentBarStarted && state->currentBarStart > 0)
		{
			// Finalize the completed 5-second candle into the cache.
			FyersUpsertQuotation(state->fiveSecBars, state->currentBar,
			                     FYERS_MAX_5S_BARS);
			state->lastCompleted5s = state->currentBarStart;
		}

		memset(&state->currentBar, 0, sizeof(state->currentBar));
		ConvertUnixToPackedDate(barStart, &state->currentBar.DateTime);
		state->currentBar.Open = ltp;
		state->currentBar.High = ltp;
		state->currentBar.Low = ltp;
		state->currentBar.Price = ltp;
		state->currentBar.Volume = max(0.0f, volumeDelta);
		state->currentBar.OpenInterest = oi;
		state->currentBarStart = barStart;
		state->currentBarStarted = TRUE;
	}
	else
	{
		if (ltp > state->currentBar.High) state->currentBar.High = ltp;
		if (ltp < state->currentBar.Low || state->currentBar.Low == 0.0f)
			state->currentBar.Low = ltp;
		state->currentBar.Price = ltp;
		state->currentBar.Volume += max(0.0f, volumeDelta);
		if (oi > 0) state->currentBar.OpenInterest = oi;
	}

	state->lastTradeTs = tradeTs;
	state->lastLocalTickTick = (DWORD)GetTickCount64();

	LeaveCriticalSection(&g_fyersStateCS);

	FyersUpdateRecentInfo(ticker, ltp, open, high, low, prevClose,
	                      totalVol, oi, bid, ask, tradeQty, isTrade);

	// Repair after releasing the state lock.
	if (gapStart > 0 && gapEnd >= gapStart)
	{
		// The provider only guarantees recent seconds history. The history
		// worker will fetch what is available and merge it chronologically.
		FyersHistoryJob job;
		job.ticker = ticker;
		job.periodicity = FYERS_PERIOD_5SEC;
		job.startTs = gapStart;
		job.endTs = gapEnd;
		job.initialLoad = FALSE;

		BOOL queueJob = FALSE;
		EnterCriticalSection(&g_fyersStateCS);
		if (!state->fiveSecFetchInProgress)
		{
			state->fiveSecFetchInProgress = TRUE;
			queueJob = TRUE;
		}
		LeaveCriticalSection(&g_fyersStateCS);

		if (queueJob && g_fyersHistoryQueueCSInitialized)
		{
			EnterCriticalSection(&g_fyersHistoryQueueCS);
			g_fyersHistoryQueue.Add(job);
			LeaveCriticalSection(&g_fyersHistoryQueueCS);
			if (g_hFyersHistoryEvent) SetEvent(g_hFyersHistoryEvent);
		}
	}

	if (g_hAmiBrokerWnd != NULL)
		PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
}

static void FyersProcessWebSocketPayload(const CStringA& payloadA)
{
	if (payloadA.IsEmpty())
		return;

	CString payload(payloadA);

	// A close or subscription ACK has no market-data symbol + ltp pair.
	if (payload.Find(_T("\"symbol\"")) < 0)
		return;

	FyersHandleMarketMessage(payload);
}

static void FyersCloseWebSocketOnOwnerThread(void)
{
	HINTERNET ws = NULL;
	HINTERNET conn = NULL;
	HINTERNET session = NULL;

	EnterCriticalSection(&g_fyersWsCS);
	ws = g_hFyersWebSocket;
	conn = g_hFyersWsConnection;
	session = g_hFyersWsSession;
	g_hFyersWebSocket = NULL;
	g_hFyersWsConnection = NULL;
	g_hFyersWsSession = NULL;
	LeaveCriticalSection(&g_fyersWsCS);

	if (ws)
	{
		EnterCriticalSection(&g_fyersWsIoCS);
		WinHttpWebSocketClose(ws, WINHTTP_WEB_SOCKET_SUCCESS_CLOSE_STATUS, NULL, 0);
		LeaveCriticalSection(&g_fyersWsIoCS);
		WinHttpCloseHandle(ws);
	}
	if (conn) WinHttpCloseHandle(conn);
	if (session) WinHttpCloseHandle(session);
}

static HINTERNET FyersConnectWebSocket(void)
{
	HINTERNET session = NULL;
	HINTERNET conn = NULL;
	HINTERNET request = NULL;
	HINTERNET ws = NULL;

	session = WinHttpOpen(
		L"OpenAlgo AmiBroker / FYERS Direct",
		WINHTTP_ACCESS_TYPE_AUTOMATIC_PROXY,
		WINHTTP_NO_PROXY_NAME,
		WINHTTP_NO_PROXY_BYPASS,
		0);
	if (!session)
		goto fail;

	WinHttpSetTimeouts(session,
	                   5000,
	                   5000,
	                   FYERS_WS_RECEIVE_TIMEOUT_MS,
	                   FYERS_WS_RECEIVE_TIMEOUT_MS);

	conn = WinHttpConnect(session, FYERS_DIRECT_WS_HOST,
	                      INTERNET_DEFAULT_HTTPS_PORT, 0);
	if (!conn)
		goto fail;

	request = WinHttpOpenRequest(
		conn,
		L"GET",
		FYERS_DIRECT_WS_PATH,
		NULL,
		WINHTTP_NO_REFERER,
		WINHTTP_DEFAULT_ACCEPT_TYPES,
		WINHTTP_FLAG_SECURE);
	if (!request)
		goto fail;

	if (!WinHttpSetOption(request, WINHTTP_OPTION_UPGRADE_TO_WEB_SOCKET, NULL, 0))
		goto fail;

	CString auth;
	auth.Format(_T("Authorization: %s:%s\r\n"),
		(LPCTSTR)FyersNormalizeAppId(g_fyersAppId),
		(LPCTSTR)g_fyersAccessToken);

	if (!WinHttpAddRequestHeaders(request, auth, (DWORD)-1L,
	                              WINHTTP_ADDREQ_FLAG_ADD | WINHTTP_ADDREQ_FLAG_REPLACE))
		goto fail;

	if (!WinHttpSendRequest(request,
	                        WINHTTP_NO_ADDITIONAL_HEADERS, 0,
	                        WINHTTP_NO_REQUEST_DATA, 0, 0, 0))
		goto fail;

	if (!WinHttpReceiveResponse(request, NULL))
		goto fail;

	DWORD status = 0;
	DWORD size = sizeof(status);
	if (!WinHttpQueryHeaders(
		request,
		WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER,
		WINHTTP_HEADER_NAME_BY_INDEX,
		&status,
		&size,
		WINHTTP_NO_HEADER_INDEX))
		goto fail;

	if (status != 101)
	{
		CString msg;
		msg.Format(_T("FYERS WebSocket HTTP handshake returned %lu"), status);
		OutputDebugString(msg);
		goto fail;
	}

	ws = WinHttpWebSocketCompleteUpgrade(request, 0);
	WinHttpCloseHandle(request);
	request = NULL;

	if (!ws)
		goto fail;

	EnterCriticalSection(&g_fyersWsCS);
	g_hFyersWebSocket = ws;
	g_hFyersWsConnection = conn;
	g_hFyersWsSession = session;
	LeaveCriticalSection(&g_fyersWsCS);

	// Ownership moved to the global websocket state.
	conn = NULL;
	session = NULL;

	SetEvent(g_hFyersConnectedEvent);
	return ws;

fail:
	if (request) WinHttpCloseHandle(request);
	if (conn) WinHttpCloseHandle(conn);
	if (session) WinHttpCloseHandle(session);
	return NULL;
}

static UINT __cdecl FyersWebSocketThreadProc(LPVOID)
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());

	CStringA fragmentBuffer;
	ULONGLONG lastHeartbeat = GetTickCount64();

	while (InterlockedCompareExchange(&g_bFyersWsShouldStop, 0, 0) == 0)
	{
		if (InterlockedExchange(&g_bFyersForceReconnect, 0) != 0)
		{
			FyersCloseWebSocketOnOwnerThread();
			ResetEvent(g_hFyersConnectedEvent);
		}

		HINTERNET ws = NULL;
		EnterCriticalSection(&g_fyersWsCS);
		ws = g_hFyersWebSocket;
		LeaveCriticalSection(&g_fyersWsCS);

		if (!ws)
		{
			if (!FyersCredentialsReady())
			{
				g_nStatus = STATUS_WAIT;
				Sleep(FYERS_RECONNECT_DELAY_MS);
				continue;
			}

			g_nStatus = STATUS_WAIT;
			ws = FyersConnectWebSocket();
			if (!ws)
			{
				g_nStatus = STATUS_DISCONNECTED;
				Sleep(FYERS_RECONNECT_DELAY_MS);
				continue;
			}

			g_nStatus = STATUS_CONNECTED;
			FyersSubscribeAll();

			if (g_hAmiBrokerWnd)
				PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
		}

		BYTE buffer[65536];
		DWORD received = 0;
		WINHTTP_WEB_SOCKET_BUFFER_TYPE bufferType = WINHTTP_WEB_SOCKET_UTF8_MESSAGE_BUFFER_TYPE;

		DWORD rc = WinHttpWebSocketReceive(ws, buffer, sizeof(buffer) - 1,
		                                   &received, &bufferType);

		if (rc == NO_ERROR)
		{
			if (bufferType == WINHTTP_WEB_SOCKET_UTF8_FRAGMENT_BUFFER_TYPE)
			{
				fragmentBuffer.Append((LPCSTR)buffer, (int)received);
				continue;
			}
			if (bufferType == WINHTTP_WEB_SOCKET_UTF8_MESSAGE_BUFFER_TYPE)
			{
				if (!fragmentBuffer.IsEmpty())
				{
					fragmentBuffer.Append((LPCSTR)buffer, (int)received);
					FyersProcessWebSocketPayload(fragmentBuffer);
					fragmentBuffer.Empty();
				}
				else
				{
					CStringA msg((LPCSTR)buffer, (int)received);
					FyersProcessWebSocketPayload(msg);
				}
				lastHeartbeat = GetTickCount64();
				continue;
			}
			if (bufferType == WINHTTP_WEB_SOCKET_PING_BUFFER_TYPE)
			{
				EnterCriticalSection(&g_fyersWsIoCS);
				WinHttpWebSocketSend(ws,
					WINHTTP_WEB_SOCKET_PONG_BUFFER_TYPE,
					buffer,
					received);
				LeaveCriticalSection(&g_fyersWsIoCS);
				continue;
			}
			if (bufferType == WINHTTP_WEB_SOCKET_PONG_BUFFER_TYPE)
				continue;
			if (bufferType == WINHTTP_WEB_SOCKET_CLOSE_BUFFER_TYPE)
			{
				FyersCloseWebSocketOnOwnerThread();
				ResetEvent(g_hFyersConnectedEvent);
				g_nStatus = STATUS_DISCONNECTED;
				continue;
			}
		}
		else if (rc == ERROR_WINHTTP_TIMEOUT)
		{
			ULONGLONG now = GetTickCount64();
			if ((now - lastHeartbeat) >= (ULONGLONG)FYERS_WS_HEARTBEAT_SEC * 1000ULL)
			{
				CStringA ping("ping");
				FyersSendWebSocketUtf8(ping);
				lastHeartbeat = now;
			}
			continue;
		}
		else
		{
			FyersCloseWebSocketOnOwnerThread();
			ResetEvent(g_hFyersConnectedEvent);
			g_nStatus = STATUS_DISCONNECTED;
		}
	}

	FyersCloseWebSocketOnOwnerThread();
	ResetEvent(g_hFyersConnectedEvent);
	g_nStatus = STATUS_SHUTDOWN;
	return 0;
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
	g_fyersHistoryQueue.Add(job);
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

static int FyersDirectGetQuotesEx(LPCTSTR pszTicker,
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

static BOOL FyersTestWebSocket(void)
{
	if (!FyersCredentialsReady())
		return FALSE;

	InterlockedExchange(&g_bFyersForceReconnect, 1);

	if (g_hFyersConnectedEvent &&
	    WaitForSingleObject(g_hFyersConnectedEvent, 3000) == WAIT_OBJECT_0)
		return TRUE;

	return FALSE;
}

static void FyersRequestReconnect(void)
{
	InterlockedExchange(&g_bFyersForceReconnect, 1);
}

static BOOL FyersDirectInit(void)
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

	if (!FyersLoadSettings())
		FyersLoadLegacyAccessTokenFile();

	if (!g_fyersAppId.IsEmpty())
		g_fyersAppId = FyersNormalizeAppId(g_fyersAppId);

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

static void FyersDirectRelease(void)
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

static BOOL FyersDirectNotify(struct PluginNotification* pn)
{
	if (!pn)
		return 0;

	if (pn->nReason & REASON_DATABASE_LOADED)
	{
		g_hAmiBrokerWnd = pn->hMainWnd;
		g_nStatus = FyersCredentialsReady() ? STATUS_WAIT : STATUS_DISCONNECTED;

		// Any old symbol state from a previous database can safely be discarded;
		// AmiBroker will ask us for the new database's symbols.
		FyersClearStates();
	}
	
	if (pn->nReason & REASON_DATABASE_UNLOADED)
	{
		g_hAmiBrokerWnd = NULL;
		FyersClearStates();
	}

	if (pn->nReason & REASON_SETTINGS_CHANGE)
	{
		FyersRequestReconnect();
	}

	return 1;
}

static BOOL FyersDirectReconfigure(CString appId, CString accessToken)
{
	appId.Trim();
	accessToken.Trim();

	if (appId.IsEmpty() || accessToken.IsEmpty())
		return FALSE;

	g_fyersAppId = FyersNormalizeAppId(appId);
	g_fyersAccessToken = accessToken;

	if (!FyersSaveSettings())
		return FALSE;

	FyersRequestReconnect();
	return TRUE;
}
