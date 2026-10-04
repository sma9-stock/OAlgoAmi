// FyersPythonBridge.inc
// -----------------------------------------------------------------------------
// AmiBroker-side adapter for FyersBridge.py.
// The DLL never connects to FYERS. Python uses the official FYERS API v3 SDK.
// Local protocol is UTF-8/ASCII line-based over 127.0.0.1:47321.
//
// DLL -> Python:
//   HELLO, SUB, SYNC, TESTREST, TESTWS, PING, SHUTDOWN
// Python -> DLL:
//   READY, STATUS, AUTH_EXPIRED, QUOTE, BAR, HIST, HIST_DONE,
//   TESTREST_OK/ERR, TESTWS_OK/ERR, ERROR
//
// No order/trading API is used by this implementation.

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <process.h>
#include <vector>
#include <algorithm>
#include <stdint.h>
#include <stdio.h>

#pragma comment(lib, "ws2_32.lib")

#define FYERS_BRIDGE_PORT             47321
#define FYERS_BRIDGE_MAX_5S_BARS      180000
#define FYERS_BRIDGE_MAX_DAILY_BARS   1000
#define FYERS_BRIDGE_RETRY_MS         1000
#define FYERS_BRIDGE_WATCHDOG_MS      2000
#define FYERS_BRIDGE_POST_MS          100

struct BridgeSymbolState
{
    CString ticker;
    CArray<Quotation, Quotation> fiveSecBars;
    CArray<Quotation, Quotation> dailyBars;
    Quotation currentBar;
    BOOL currentBarStarted;
    time_t currentBarStart;
    time_t lastCompleted5s;
    time_t lastDataTs;
    BOOL subscribed;
    BOOL syncRequested;

    float ltp;
    float prevClose;
    float open;
    float high;
    float low;
    float volume;
    float oi;
    float bid;
    float ask;
    float tradeQty;

    BridgeSymbolState()
        : currentBarStarted(FALSE), currentBarStart(0), lastCompleted5s(0),
          lastDataTs(0), subscribed(FALSE), syncRequested(FALSE),
          ltp(0), prevClose(0), open(0), high(0), low(0), volume(0),
          oi(0), bid(0), ask(0), tradeQty(0)
    {
        memset(&currentBar, 0, sizeof(currentBar));
    }
};

static CMap<CString, LPCTSTR, BridgeSymbolState*, BridgeSymbolState*> g_bridgeStates;
static CRITICAL_SECTION g_bridgeStateCS;
static BOOL g_bridgeStateCSInit = FALSE;

static CMap<CString, LPCTSTR, RecentInfo*, RecentInfo*> g_bridgeRecentMap;
static CRITICAL_SECTION g_bridgeRecentCS;
static BOOL g_bridgeRecentCSInit = FALSE;

static CRITICAL_SECTION g_bridgeSocketCS;
static BOOL g_bridgeSocketCSInit = FALSE;

static CRITICAL_SECTION g_bridgeProcessCS;
static BOOL g_bridgeProcessCSInit = FALSE;

static CRITICAL_SECTION g_bridgeTestCS;
static BOOL g_bridgeTestCSInit = FALSE;

static SOCKET g_bridgeSocket = INVALID_SOCKET;
static HANDLE g_bridgeConnectThread = NULL;
static HANDLE g_bridgeWatchdogThread = NULL;
static HANDLE g_bridgeStopEvent = NULL;

static HANDLE g_bridgeProcess = NULL;
static DWORD g_bridgeProcessId = 0;
static BOOL g_bridgeStopping = FALSE;

static WSADATA g_bridgeWsaData;
static BOOL g_bridgeWsaInit = FALSE;

static HANDLE g_bridgeTestEvent = NULL;
static int g_bridgeTestKind = 0;
static BOOL g_bridgeTestResult = FALSE;
static CString g_bridgeTestMessage;

static ULONGLONG g_bridgeLastPostTick = 0;

static BOOL FyersBridgeCredentialsReady();

static CString BridgeModuleDir()
{
    HMODULE h = NULL;
    WCHAR buf[MAX_PATH] = {0};

    if (!GetModuleHandleExW(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS |
                            GET_MODULE_HANDLE_EX_FLAG_UNCHANGED_REFCOUNT,
                            reinterpret_cast<LPCWSTR>(&BridgeModuleDir), &h))
        return CString(_T("."));

    DWORD n = GetModuleFileNameW(h, buf, MAX_PATH);
    if (n == 0 || n >= MAX_PATH)
        return CString(_T("."));

    CStringW full(buf);
    int slash = full.ReverseFind(L'\\');
    if (slash < 0)
        return CString(_T("."));

    return CString(full.Left(slash));
}

static CString BridgeResolvePath(const CString& path)
{
    CString p = path;
    p.Trim();

    if (p.IsEmpty())
        return p;

    if (p.GetLength() >= 2 && p[1] == _T(':'))
        return p;
    if (p.Left(2) == _T("\\\\"))
        return p;

    return BridgeModuleDir() + _T("\\") + p;
}

static CString BridgeDefaultTokenPath()
{
    return BridgeModuleDir() + _T("\\access_token.txt");
}

static CString BridgeDefaultScriptPath()
{
    return BridgeModuleDir() + _T("\\FyersBridge.py");
}

static BOOL BridgeFileExists(const CString& path)
{
    CStringW p(path);
    DWORD attr = GetFileAttributesW((LPCWSTR)p);
    return attr != INVALID_FILE_ATTRIBUTES &&
           !(attr & FILE_ATTRIBUTE_DIRECTORY);
}

static BOOL BridgeGetFileTime(const CString& path, FILETIME& ft)
{
    WIN32_FILE_ATTRIBUTE_DATA data;
    memset(&data, 0, sizeof(data));

    CStringW p(path);
    if (!GetFileAttributesExW((LPCWSTR)p, GetFileExInfoStandard, &data))
        return FALSE;

    ft = data.ftLastWriteTime;
    return TRUE;
}

static void BridgeStatus(const CString& message)
{
    g_fyersLastError = message;
    OutputDebugString(message);
    OutputDebugString(_T("\r\n"));
}

static BridgeSymbolState* BridgeState(LPCTSTR ticker)
{
    CString key(ticker ? ticker : _T(""));
    key.Trim();
    if (key.IsEmpty())
        return NULL;

    EnterCriticalSection(&g_bridgeStateCS);

    BridgeSymbolState* state = NULL;
    if (!g_bridgeStates.Lookup(key, state) || state == NULL)
    {
        state = new BridgeSymbolState();
        state->ticker = key;
        g_bridgeStates.SetAt(key, state);
    }

    LeaveCriticalSection(&g_bridgeStateCS);
    return state;
}

static RecentInfo* BridgeRecent(LPCTSTR ticker)
{
    CString key(ticker ? ticker : _T(""));
    key.Trim();
    if (key.IsEmpty())
        return NULL;

    EnterCriticalSection(&g_bridgeRecentCS);

    RecentInfo* info = NULL;
    if (!g_bridgeRecentMap.Lookup(key, info) || info == NULL)
    {
        info = new RecentInfo();
        memset(info, 0, sizeof(RecentInfo));
        info->nStructSize = sizeof(RecentInfo);

        CStringA nameA(key);
        strncpy_s(info->Name, sizeof(info->Name), nameA, _TRUNCATE);

        int colon = key.Find(_T(':'));
        CString exchange = colon > 0 ? key.Left(colon) : _T("NSE");
        CStringA exA(exchange);
        strncpy_s(info->Exchange, sizeof(info->Exchange), exA, _TRUNCATE);

        g_bridgeRecentMap.SetAt(key, info);
    }

    LeaveCriticalSection(&g_bridgeRecentCS);
    return info;
}

static int BridgeCompareDate(const AmiDate& a, const AmiDate& b)
{
    if (a.PackDate.Year != b.PackDate.Year)
        return a.PackDate.Year < b.PackDate.Year ? -1 : 1;
    if (a.PackDate.Month != b.PackDate.Month)
        return a.PackDate.Month < b.PackDate.Month ? -1 : 1;
    if (a.PackDate.Day != b.PackDate.Day)
        return a.PackDate.Day < b.PackDate.Day ? -1 : 1;
    return 0;
}

static int BridgeFindBar(const CArray<Quotation, Quotation>& arr, DATE_TIME_INT key)
{
    int lo = 0;
    int hi = (int)arr.GetCount() - 1;

    while (lo <= hi)
    {
        int mid = lo + (hi - lo) / 2;
        DATE_TIME_INT v = arr[mid].DateTime.Date;

        if (v < key) lo = mid + 1;
        else if (v > key) hi = mid - 1;
        else return mid;
    }

    return -1;
}

static void BridgeUpsert(CArray<Quotation, Quotation>& arr,
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
        int idx = BridgeFindBar(arr, key);
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
                if (arr[mid].DateTime.Date < key)
                    lo = mid + 1;
                else
                    hi = mid;
            }
            arr.InsertAt(lo, q);
        }
    }

    if (maxBars > 0 && arr.GetCount() > maxBars)
        arr.RemoveAt(0, (int)arr.GetCount() - maxBars);
}

static BOOL BridgeSend(const CString& line)
{
    CStringA data(line);
    data += "\n";

    EnterCriticalSection(&g_bridgeSocketCS);

    SOCKET s = g_bridgeSocket;
    if (s == INVALID_SOCKET)
    {
        LeaveCriticalSection(&g_bridgeSocketCS);
        return FALSE;
    }

    const char* p = data.GetString();
    int total = data.GetLength();
    int sent = 0;

    while (sent < total)
    {
        int n = send(s, p + sent, total - sent, 0);
        if (n <= 0)
        {
            LeaveCriticalSection(&g_bridgeSocketCS);
            return FALSE;
        }
        sent += n;
    }

    LeaveCriticalSection(&g_bridgeSocketCS);
    return TRUE;
}

static void BridgePostUpdate(BOOL force)
{
    if (!g_hAmiBrokerWnd)
        return;

    ULONGLONG now = GetTickCount64();
    if (!force && g_bridgeLastPostTick != 0 &&
        now - g_bridgeLastPostTick < FYERS_BRIDGE_POST_MS)
        return;

    g_bridgeLastPostTick = now;
    PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
}

static void BridgePublishRecent(const CString& ticker)
{
    BridgeSymbolState* state = BridgeState(ticker);
    if (!state)
        return;

    RecentInfo* info = BridgeRecent(ticker);
    if (!info)
        return;

    EnterCriticalSection(&g_bridgeStateCS);
    float ltp = state->ltp;
    float prev = state->prevClose;
    float open = state->open;
    float high = state->high;
    float low = state->low;
    float vol = state->volume;
    float oi = state->oi;
    float bid = state->bid;
    float ask = state->ask;
    float qty = state->tradeQty;
    LeaveCriticalSection(&g_bridgeStateCS);

    EnterCriticalSection(&g_bridgeRecentCS);

    int bm = info->nBitmap;
    if (ltp > 0) { info->fLast = ltp; bm |= RI_LAST; }
    if (open > 0) { info->fOpen = open; bm |= RI_OPEN; }
    if (high > 0) { info->fHigh = high; bm |= RI_HIGHLOW; }
    if (low > 0) { info->fLow = low; bm |= RI_HIGHLOW; }
    if (prev > 0)
    {
        info->fPrev = prev;
        info->fChange = ltp > 0 ? ltp - prev : info->fChange;
        bm |= RI_PREVCHANGE;
    }

    info->fTotalVol = vol;
    info->iTotalVol = (int)vol;
    bm |= RI_TOTALVOL;

    if (oi > 0) { info->fOpenInt = oi; bm |= RI_OPENINT; }
    if (bid > 0) { info->fBid = bid; bm |= RI_BID; }
    if (ask > 0) { info->fAsk = ask; bm |= RI_ASK; }
    if (qty > 0)
    {
        info->fTradeVol = qty;
        info->iTradeVol = (int)qty;
        bm |= RI_TRADEVOL;
    }

    CTime now = CTime::GetCurrentTime();
    info->nDateUpdate = now.GetYear() * 10000 + now.GetMonth() * 100 + now.GetDay();
    info->nTimeUpdate = now.GetHour() * 10000 + now.GetMinute() * 100 + now.GetSecond();
    bm |= RI_DATEUPDATE;
    info->nStatus = RI_STATUS_UPDATE | RI_STATUS_BARSREADY;
    info->nBitmap = bm;

    LeaveCriticalSection(&g_bridgeRecentCS);

    BridgePostUpdate(FALSE);
}

static BOOL BridgeHandleQuote(CStringA& line, int& pos)
{
    CStringA sym = line.Tokenize("\t", pos);
    CStringA tsA = line.Tokenize("\t", pos);
    CStringA ltpA = line.Tokenize("\t", pos);
    CStringA prevA = line.Tokenize("\t", pos);
    CStringA openA = line.Tokenize("\t", pos);
    CStringA highA = line.Tokenize("\t", pos);
    CStringA lowA = line.Tokenize("\t", pos);
    CStringA volA = line.Tokenize("\t", pos);
    CStringA oiA = line.Tokenize("\t", pos);
    CStringA bidA = line.Tokenize("\t", pos);
    CStringA askA = line.Tokenize("\t", pos);
    CStringA qtyA = line.Tokenize("\t", pos);

    if (sym.IsEmpty() || tsA.IsEmpty())
        return FALSE;

    BridgeSymbolState* state = BridgeState(CString(sym));
    if (!state)
        return FALSE;

    EnterCriticalSection(&g_bridgeStateCS);
    state->lastDataTs = max(state->lastDataTs, (time_t)_strtoi64(tsA, NULL, 10));
    state->ltp = (float)atof(ltpA);
    state->prevClose = (float)atof(prevA);
    state->open = (float)atof(openA);
    state->high = (float)atof(highA);
    state->low = (float)atof(lowA);
    state->volume = (float)atof(volA);
    state->oi = (float)atof(oiA);
    state->bid = (float)atof(bidA);
    state->ask = (float)atof(askA);
    state->tradeQty = (float)atof(qtyA);
    LeaveCriticalSection(&g_bridgeStateCS);

    BridgePublishRecent(CString(sym));
    return TRUE;
}

static BOOL BridgeHandleBar(CStringA& line, int& pos)
{
    CStringA sym = line.Tokenize("\t", pos);
    CStringA tsA = line.Tokenize("\t", pos);
    CStringA oA = line.Tokenize("\t", pos);
    CStringA hA = line.Tokenize("\t", pos);
    CStringA lA = line.Tokenize("\t", pos);
    CStringA cA = line.Tokenize("\t", pos);
    CStringA vA = line.Tokenize("\t", pos);
    CStringA oiA = line.Tokenize("\t", pos);
    CStringA finalA = line.Tokenize("\t", pos);

    if (sym.IsEmpty() || tsA.IsEmpty())
        return FALSE;

    time_t ts = (time_t)_strtoi64(tsA, NULL, 10);
    if (ts <= 0)
        return FALSE;

    Quotation q;
    memset(&q, 0, sizeof(q));
    ConvertUnixToPackedDate(ts, &q.DateTime);
    q.Open = (float)atof(oA);
    q.High = (float)atof(hA);
    q.Low = (float)atof(lA);
    q.Price = (float)atof(cA);
    q.Volume = (float)atof(vA);
    q.OpenInterest = (float)atof(oiA);

    BridgeSymbolState* state = BridgeState(CString(sym));
    if (!state)
        return FALSE;

    BOOL finalBar = atoi(finalA) != 0;

    EnterCriticalSection(&g_bridgeStateCS);

    if (finalBar)
    {
        BridgeUpsert(state->fiveSecBars, q, FYERS_BRIDGE_MAX_5S_BARS);
        state->lastCompleted5s = max(state->lastCompleted5s, ts);
        state->currentBarStarted = FALSE;
        state->currentBarStart = 0;
        memset(&state->currentBar, 0, sizeof(state->currentBar));
    }
    else
    {
        state->currentBar = q;
        state->currentBarStarted = TRUE;
        state->currentBarStart = ts;
    }

    state->lastDataTs = max(state->lastDataTs, ts);

    LeaveCriticalSection(&g_bridgeStateCS);

    BridgePostUpdate(finalBar);
    return TRUE;
}

static BOOL BridgeHandleHistory(CStringA& line, int& pos)
{
    CStringA sym = line.Tokenize("\t", pos);
    CStringA res = line.Tokenize("\t", pos);
    CStringA tsA = line.Tokenize("\t", pos);
    CStringA oA = line.Tokenize("\t", pos);
    CStringA hA = line.Tokenize("\t", pos);
    CStringA lA = line.Tokenize("\t", pos);
    CStringA cA = line.Tokenize("\t", pos);
    CStringA vA = line.Tokenize("\t", pos);
    CStringA oiA = line.Tokenize("\t", pos);

    if (sym.IsEmpty() || res.IsEmpty() || tsA.IsEmpty())
        return FALSE;

    time_t ts = (time_t)_strtoi64(tsA, NULL, 10);
    if (ts <= 0)
        return FALSE;

    Quotation q;
    memset(&q, 0, sizeof(q));
    ConvertUnixToPackedDate(ts, &q.DateTime);

    if (res.CompareNoCase("D") == 0)
    {
        q.DateTime.Date |= DAILY_MASK;
        q.DateTime.PackDate.Hour = DATE_EOD_HOURS;
        q.DateTime.PackDate.Minute = DATE_EOD_MINUTES;
    }

    q.Open = (float)atof(oA);
    q.High = (float)atof(hA);
    q.Low = (float)atof(lA);
    q.Price = (float)atof(cA);
    q.Volume = (float)atof(vA);
    q.OpenInterest = (float)atof(oiA);

    BridgeSymbolState* state = BridgeState(CString(sym));
    if (!state)
        return FALSE;

    EnterCriticalSection(&g_bridgeStateCS);

    if (res.CompareNoCase("D") == 0)
    {
        BridgeUpsert(state->dailyBars, q, FYERS_BRIDGE_MAX_DAILY_BARS);
    }
    else
    {
        BridgeUpsert(state->fiveSecBars, q, FYERS_BRIDGE_MAX_5S_BARS);
        if (ts > state->lastCompleted5s)
            state->lastCompleted5s = ts;
    }

    state->lastDataTs = max(state->lastDataTs, ts);

    LeaveCriticalSection(&g_bridgeStateCS);

    return TRUE;
}

static void BridgeProcessLine(const CStringA& input)
{
    CStringA line(input);
    line.Trim();
    if (line.IsEmpty())
        return;

    int pos = 0;
    CStringA commandA = line.Tokenize("\t", pos);
    CString command(commandA);
    command.MakeUpper();

    if (command == _T("READY"))
    {
        BridgeStatus(_T("FYERS Python bridge connected."));
        g_nStatus = STATUS_WAIT;
        BridgeSend(_T("PING\t1"));
        BridgeSend(_T("HELLO\t1\t5S"));
        return;
    }

    if (command == _T("STATUS"))
    {
        CStringA stateA = line.Tokenize("\t", pos);
        CStringA msgA = line.Tokenize("\t", pos);

        CString state(stateA);
        state.MakeUpper();

        if (state == _T("CONNECTED") || state == _T("AUTHENTICATED"))
            g_nStatus = STATUS_CONNECTED;
        else if (state == _T("DISCONNECTED") || state == _T("ERROR"))
            g_nStatus = STATUS_DISCONNECTED;
        else
            g_nStatus = STATUS_WAIT;

        CString msg;
        msg.Format(_T("FYERS Python bridge: %s%s%s"),
                   (LPCTSTR)state,
                   msgA.IsEmpty() ? _T("") : _T(" - "),
                   (LPCTSTR)CString(msgA));
        BridgeStatus(msg);
        BridgePostUpdate(TRUE);
        return;
    }

    if (command == _T("AUTH_EXPIRED"))
    {
        g_nStatus = STATUS_DISCONNECTED;
        BridgeStatus(_T("FYERS access token expired. Run atok.py to replace access_token.txt; reconnect will occur automatically."));
        BridgePostUpdate(TRUE);
        return;
    }

    if (command == _T("QUOTE"))
    {
        BridgeHandleQuote(line, pos);
        return;
    }

    if (command == _T("BAR"))
    {
        BridgeHandleBar(line, pos);
        return;
    }

    if (command == _T("HIST"))
    {
        BridgeHandleHistory(line, pos);
        return;
    }

    if (command == _T("HIST_DONE"))
    {
        BridgePostUpdate(TRUE);
        return;
    }

    if (command == _T("TESTREST_OK"))
    {
        CStringA msgA = line.Tokenize("\t", pos);
        EnterCriticalSection(&g_bridgeTestCS);
        g_bridgeTestKind = 1;
        g_bridgeTestResult = TRUE;
        g_bridgeTestMessage = CString(msgA);
        LeaveCriticalSection(&g_bridgeTestCS);
        SetEvent(g_bridgeTestEvent);
        return;
    }

    if (command == _T("TESTREST_ERR"))
    {
        CStringA msgA = line.Tokenize("\t", pos);
        EnterCriticalSection(&g_bridgeTestCS);
        g_bridgeTestKind = 1;
        g_bridgeTestResult = FALSE;
        g_bridgeTestMessage = CString(msgA);
        LeaveCriticalSection(&g_bridgeTestCS);
        SetEvent(g_bridgeTestEvent);
        return;
    }

    if (command == _T("TESTWS_OK"))
    {
        CStringA msgA = line.Tokenize("\t", pos);
        EnterCriticalSection(&g_bridgeTestCS);
        g_bridgeTestKind = 2;
        g_bridgeTestResult = TRUE;
        g_bridgeTestMessage = CString(msgA);
        LeaveCriticalSection(&g_bridgeTestCS);
        SetEvent(g_bridgeTestEvent);
        return;
    }

    if (command == _T("TESTWS_ERR"))
    {
        CStringA msgA = line.Tokenize("\t", pos);
        EnterCriticalSection(&g_bridgeTestCS);
        g_bridgeTestKind = 2;
        g_bridgeTestResult = FALSE;
        g_bridgeTestMessage = CString(msgA);
        LeaveCriticalSection(&g_bridgeTestCS);
        SetEvent(g_bridgeTestEvent);
        return;
    }

    if (command == _T("ERROR"))
    {
        CStringA codeA = line.Tokenize("\t", pos);
        CStringA msgA = line.Tokenize("\t", pos);
        CString msg;
        msg.Format(_T("FYERS bridge error %s: %s"),
                   (LPCTSTR)CString(codeA), (LPCTSTR)CString(msgA));
        BridgeStatus(msg);
        g_nStatus = STATUS_DISCONNECTED;
        BridgePostUpdate(TRUE);
        return;
    }

    if (command == _T("PONG"))
        return;
}

static void BridgeSendCurrentSubscriptions()
{
    EnterCriticalSection(&g_bridgeStateCS);

    std::vector<CString> symbols;
    POSITION pos = g_bridgeStates.GetStartPosition();
    while (pos != NULL)
    {
        CString key;
        BridgeSymbolState* state = NULL;
        g_bridgeStates.GetNextAssoc(pos, key, state);
        if (state)
            symbols.push_back(state->ticker);
    }

    LeaveCriticalSection(&g_bridgeStateCS);

    for (size_t i = 0; i < symbols.size(); ++i)
    {
        CString msg;
        msg.Format(_T("SUB\t%s"), (LPCTSTR)symbols[i]);
        BridgeSend(msg);

        BridgeSymbolState* state = BridgeState(symbols[i]);
        if (state)
        {
            EnterCriticalSection(&g_bridgeStateCS);
            time_t last = state->lastCompleted5s;
            state->syncRequested = TRUE;
            LeaveCriticalSection(&g_bridgeStateCS);

            msg.Format(_T("SYNC\t%s\t%lld"),
                       (LPCTSTR)symbols[i], (long long)last);
            BridgeSend(msg);
        }
    }
}

static void BridgeCloseSocket()
{
    EnterCriticalSection(&g_bridgeSocketCS);
    SOCKET s = g_bridgeSocket;
    g_bridgeSocket = INVALID_SOCKET;
    LeaveCriticalSection(&g_bridgeSocketCS);

    if (s != INVALID_SOCKET)
    {
        shutdown(s, SD_BOTH);
        closesocket(s);
    }

    if (!g_bridgeStopping)
        g_nStatus = STATUS_DISCONNECTED;
}

static UINT BridgeConnectThreadProc(LPVOID)
{
    while (!g_bridgeStopping)
    {
        SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (s == INVALID_SOCKET)
        {
            Sleep(FYERS_BRIDGE_RETRY_MS);
            continue;
        }

        sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = htons(FYERS_BRIDGE_PORT);

        if (connect(s, (sockaddr*)&addr, sizeof(addr)) != 0)
        {
            closesocket(s);
            Sleep(FYERS_BRIDGE_RETRY_MS);
            continue;
        }

        DWORD timeout = 1000;
        setsockopt(s, SOL_SOCKET, SO_RCVTIMEO,
                   (const char*)&timeout, sizeof(timeout));

        EnterCriticalSection(&g_bridgeSocketCS);
        g_bridgeSocket = s;
        LeaveCriticalSection(&g_bridgeSocketCS);

        BridgeStatus(_T("Connected to local FyersBridge.py."));
        g_nStatus = STATUS_WAIT;

        BridgeSend(_T("HELLO\t1\t5S"));
        BridgeSendCurrentSubscriptions();

        char buffer[65536];
        CStringA pending;

        while (!g_bridgeStopping)
        {
            int n = recv(s, buffer, sizeof(buffer), 0);
            if (n == 0)
                break;

            if (n < 0)
            {
                int err = WSAGetLastError();
                if (err == WSAETIMEDOUT || err == WSAEINTR)
                    continue;
                break;
            }

            pending.Append(buffer, n);

            int newline = -1;
            while ((newline = pending.Find('\n')) >= 0)
            {
                CStringA line = pending.Left(newline);
                pending = pending.Mid(newline + 1);
                line.Trim();

                if (!line.IsEmpty())
                    BridgeProcessLine(line);
            }
        }

        EnterCriticalSection(&g_bridgeSocketCS);
        if (g_bridgeSocket == s)
            g_bridgeSocket = INVALID_SOCKET;
        LeaveCriticalSection(&g_bridgeSocketCS);

        shutdown(s, SD_BOTH);
        closesocket(s);

        if (!g_bridgeStopping)
        {
            g_nStatus = STATUS_DISCONNECTED;
            BridgePostUpdate(TRUE);
            Sleep(FYERS_BRIDGE_RETRY_MS);
        }
    }

    return 0;
}

static BOOL BridgeLaunchPython()
{
    if (g_bridgeStopping)
        return FALSE;

    if (g_fyersAppId.IsEmpty())
    {
        BridgeStatus(_T("FYERS App ID is not configured."));
        return FALSE;
    }

    if (!BridgeFileExists(g_fyersTokenPath))
    {
        BridgeStatus(_T("access_token.txt is missing. Run atok.py first."));
        return FALSE;
    }

    if (!BridgeFileExists(g_bridgeScriptPath))
    {
        BridgeStatus(_T("FyersBridge.py is missing beside OpenAlgo.dll."));
        return FALSE;
    }

    EnterCriticalSection(&g_bridgeProcessCS);
    if (g_bridgeProcess != NULL)
    {
        DWORD rc = WaitForSingleObject(g_bridgeProcess, 0);
        if (rc == WAIT_TIMEOUT)
        {
            LeaveCriticalSection(&g_bridgeProcessCS);
            return TRUE;
        }

        CloseHandle(g_bridgeProcess);
        g_bridgeProcess = NULL;
        g_bridgeProcessId = 0;
    }
    LeaveCriticalSection(&g_bridgeProcessCS);

    WCHAR pyPath[MAX_PATH] = {0};
    DWORD n = SearchPathW(NULL, L"py.exe", NULL, MAX_PATH, pyPath, NULL);
    BOOL usingPyLauncher = (n > 0 && n < MAX_PATH);

    if (!usingPyLauncher)
    {
        memset(pyPath, 0, sizeof(pyPath));
        n = SearchPathW(NULL, L"python.exe", NULL, MAX_PATH, pyPath, NULL);
    }

    if (n == 0 || n >= MAX_PATH)
    {
        BridgeStatus(_T("Could not find py.exe or python.exe in PATH."));
        return FALSE;
    }

    CStringW cmd;
    if (usingPyLauncher)
        cmd.Format(L"\\\"%s\\\" -3 \\\"%s\\\" --app-id \\\"%s\\\" --token-file \\\"%s\\\" --host 127.0.0.1 --port %d",
                   pyPath,
                   (LPCWSTR)CStringW(g_bridgeScriptPath),
                   (LPCWSTR)CStringW(g_fyersAppId),
                   (LPCWSTR)CStringW(g_bridgeTokenPath),
                   FYERS_BRIDGE_PORT);
    else
        cmd.Format(L"\\\"%s\\\" \\\"%s\\\" --app-id \\\"%s\\\" --token-file \\\"%s\\\" --host 127.0.0.1 --port %d",
                   pyPath,
                   (LPCWSTR)CStringW(g_bridgeScriptPath),
                   (LPCWSTR)CStringW(g_fyersAppId),
                   (LPCWSTR)CStringW(g_bridgeTokenPath),
                   FYERS_BRIDGE_PORT);

    std::vector<wchar_t> mutableCmd((size_t)cmd.GetLength() + 1);
    wcscpy_s(mutableCmd.data(), mutableCmd.size(), cmd);

    STARTUPINFOW si;
    PROCESS_INFORMATION pi;
    memset(&si, 0, sizeof(si));
    memset(&pi, 0, sizeof(pi));
    si.cb = sizeof(si);
    si.dwFlags = STARTF_USESHOWWINDOW;
    si.wShowWindow = SW_HIDE;

    CStringW workDir(BridgeModuleDir());

    if (!CreateProcessW(NULL, mutableCmd.data(), NULL, NULL, FALSE,
                        CREATE_NO_WINDOW, NULL,
                        (LPCWSTR)workDir, &si, &pi))
    {
        CString msg;
        msg.Format(_T("Could not start FyersBridge.py (Windows error %lu)."),
                   (unsigned long)GetLastError());
        BridgeStatus(msg);
        return FALSE;
    }

    CloseHandle(pi.hThread);

    EnterCriticalSection(&g_bridgeProcessCS);
    g_bridgeProcess = pi.hProcess;
    g_bridgeProcessId = pi.dwProcessId;
    LeaveCriticalSection(&g_bridgeProcessCS);

    BridgeStatus(_T("Starting FyersBridge.py with the official FYERS Python SDK."));
    return TRUE;
}

static void BridgeStopPython()
{
    BridgeSend(_T("SHUTDOWN"));

    EnterCriticalSection(&g_bridgeProcessCS);
    HANDLE p = g_bridgeProcess;
    g_bridgeProcess = NULL;
    g_bridgeProcessId = 0;
    LeaveCriticalSection(&g_bridgeProcessCS);

    if (p)
    {
        if (WaitForSingleObject(p, 2500) == WAIT_TIMEOUT)
            TerminateProcess(p, 0);

        WaitForSingleObject(p, 1000);
        CloseHandle(p);
    }
}

static UINT BridgeWatchdogThreadProc(LPVOID)
{
    FILETIME lastTokenTime;
    memset(&lastTokenTime, 0, sizeof(lastTokenTime));

    BridgeGetFileTime(g_bridgeTokenPath, lastTokenTime);

    while (!g_bridgeStopping)
    {
        Sleep(FYERS_BRIDGE_WATCHDOG_MS);
        if (g_bridgeStopping)
            break;

        FILETIME nowTokenTime;
        if (BridgeGetFileTime(g_bridgeTokenPath, nowTokenTime))
        {
            BOOL changed =
                lastTokenTime.dwLowDateTime != 0 &&
                CompareFileTime(&lastTokenTime, &nowTokenTime) != 0;

            if (changed)
            {
                lastTokenTime = nowTokenTime;
                BridgeStatus(_T("access_token.txt changed — restarting the Python bridge with the new daily token."));
                BridgeStopPython();
                Sleep(500);
            }
            else
            {
                lastTokenTime = nowTokenTime;
            }
        }

        BOOL needsLaunch = FALSE;

        EnterCriticalSection(&g_bridgeProcessCS);
        if (g_bridgeProcess == NULL)
        {
            needsLaunch = TRUE;
        }
        else
        {
            DWORD rc = WaitForSingleObject(g_bridgeProcess, 0);
            if (rc == WAIT_OBJECT_0)
            {
                CloseHandle(g_bridgeProcess);
                g_bridgeProcess = NULL;
                g_bridgeProcessId = 0;
                needsLaunch = TRUE;
            }
        }
        LeaveCriticalSection(&g_bridgeProcessCS);

        if (needsLaunch && FyersBridgeCredentialsReady())
            BridgeLaunchPython();

        if (g_bridgeStopping)
            break;
    }

    return 0;
}

static int BridgeCopyData(BridgeSymbolState* state,
                          int nSize,
                          struct Quotation* pQuotes)
{
    if (!state || !pQuotes || nSize <= 0)
        return 0;

    EnterCriticalSection(&g_bridgeStateCS);

    int d = (int)state->dailyBars.GetCount();
    int s = (int)state->fiveSecBars.GetCount();

    int dailyUse = d;
    if (d > 0 && s > 0)
    {
        AmiDate first5s = state->fiveSecBars[0].DateTime;
        dailyUse = 0;

        for (int i = 0; i < d; ++i)
        {
            if (BridgeCompareDate(state->dailyBars[i].DateTime, first5s) < 0)
                dailyUse = i + 1;
            else
                break;
        }
    }

    int total = dailyUse + s;
    int start = max(0, total - nSize);
    int out = 0;

    for (int i = 0; i < dailyUse && out < nSize; ++i)
        if (i >= start)
            pQuotes[out++] = state->dailyBars[i];

    for (int i = 0; i < s && out < nSize; ++i)
        if (dailyUse + i >= start)
            pQuotes[out++] = state->fiveSecBars[i];

    if (state->currentBarStarted && out < nSize)
    {
        DATE_TIME_INT key = state->currentBar.DateTime.Date;
        BOOL replaced = FALSE;

        for (int i = out - 1; i >= 0; --i)
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

        if (!replaced)
            pQuotes[out++] = state->currentBar;
    }

    LeaveCriticalSection(&g_bridgeStateCS);
    return out;
}

BOOL FyersBridgeCredentialsReady()
{
    return !g_fyersAppId.IsEmpty() && BridgeFileExists(g_bridgeTokenPath);
}

BOOL FyersDirectReconfigure(CString appId, CString tokenFilePath)
{
    appId.Trim();
    tokenFilePath.Trim();

    if (appId.IsEmpty())
        return FALSE;

    g_fyersAppId = appId;

    g_bridgeTokenPath = tokenFilePath.IsEmpty()
        ? BridgeDefaultTokenPath()
        : BridgeResolvePath(tokenFilePath);

    g_bridgeScriptPath = BridgeDefaultScriptPath();
    g_oServer = g_fyersAppId;
    g_oApiKey = g_bridgeTokenPath;
    g_fyersAccessToken.Empty();

    CString cfg = BridgeModuleDir() + _T("\\FyersBridge.config");
    CStdioFile file;
    if (!file.Open(cfg, CFile::modeCreate | CFile::modeWrite | CFile::typeText))
        return FALSE;

    CString line;
    line.Format(_T("AppId=%s\n"), (LPCTSTR)g_fyersAppId);
    file.WriteString(line);
    line.Format(_T("TokenFile=%s\n"), (LPCTSTR)g_bridgeTokenPath);
    file.WriteString(line);
    line.Format(_T("BridgeScript=%s\n"), (LPCTSTR)g_bridgeScriptPath);
    file.WriteString(line);
    file.Close();

    BridgeStopPython();
    BridgeLaunchPython();
    return TRUE;
}

BOOL FyersTestRestConnection()
{
    if (!FyersBridgeCredentialsReady())
    {
        BridgeStatus(_T("REST test requires the FYERS App ID and access_token.txt."));
        return FALSE;
    }

    if (!BridgeLaunchPython())
        return FALSE;

    EnterCriticalSection(&g_bridgeTestCS);
    g_bridgeTestKind = 0;
    g_bridgeTestResult = FALSE;
    g_bridgeTestMessage.Empty();
    LeaveCriticalSection(&g_bridgeTestCS);

    ResetEvent(g_bridgeTestEvent);
    BridgeSend(_T("TESTREST"));

    if (WaitForSingleObject(g_bridgeTestEvent, 10000) != WAIT_OBJECT_0)
    {
        BridgeStatus(_T("REST test timed out waiting for FyersBridge.py."));
        return FALSE;
    }

    EnterCriticalSection(&g_bridgeTestCS);
    BOOL ok = g_bridgeTestKind == 1 && g_bridgeTestResult;
    CString message = g_bridgeTestMessage;
    LeaveCriticalSection(&g_bridgeTestCS);

    if (!ok && !message.IsEmpty())
        BridgeStatus(message);

    return ok;
}

BOOL FyersTestWebSocket()
{
    if (!FyersBridgeCredentialsReady())
    {
        BridgeStatus(_T("WebSocket test requires the FYERS App ID and access_token.txt."));
        return FALSE;
    }

    if (!BridgeLaunchPython())
        return FALSE;

    if (g_nStatus == STATUS_CONNECTED)
        return TRUE;

    EnterCriticalSection(&g_bridgeTestCS);
    g_bridgeTestKind = 0;
    g_bridgeTestResult = FALSE;
    g_bridgeTestMessage.Empty();
    LeaveCriticalSection(&g_bridgeTestCS);

    ResetEvent(g_bridgeTestEvent);
    BridgeSend(_T("TESTWS"));

    if (WaitForSingleObject(g_bridgeTestEvent, 10000) != WAIT_OBJECT_0)
    {
        BridgeStatus(_T("WebSocket test timed out waiting for the official FYERS Python SDK."));
        return FALSE;
    }

    EnterCriticalSection(&g_bridgeTestCS);
    BOOL ok = g_bridgeTestKind == 2 && g_bridgeTestResult;
    CString message = g_bridgeTestMessage;
    LeaveCriticalSection(&g_bridgeTestCS);

    if (!ok && !message.IsEmpty())
        BridgeStatus(message);

    return ok;
}

void FyersRequestReconnect()
{
    BridgeStopPython();
    Sleep(500);
    BridgeLaunchPython();
}

BOOL FyersDirectInit()
{
    if (!g_bridgeStateCSInit)
    {
        InitializeCriticalSection(&g_bridgeStateCS);
        g_bridgeStateCSInit = TRUE;
        g_bridgeStates.InitHashTable(503);
    }

    if (!g_bridgeRecentCSInit)
    {
        InitializeCriticalSection(&g_bridgeRecentCS);
        g_bridgeRecentCSInit = TRUE;
        g_bridgeRecentMap.InitHashTable(503);
    }

    if (!g_bridgeSocketCSInit)
    {
        InitializeCriticalSection(&g_bridgeSocketCS);
        g_bridgeSocketCSInit = TRUE;
    }

    if (!g_bridgeProcessCSInit)
    {
        InitializeCriticalSection(&g_bridgeProcessCS);
        g_bridgeProcessCSInit = TRUE;
    }

    if (!g_bridgeTestCSInit)
    {
        InitializeCriticalSection(&g_bridgeTestCS);
        g_bridgeTestCSInit = TRUE;
    }

    if (!g_bridgeStopEvent)
        g_bridgeStopEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
    if (!g_bridgeTestEvent)
        g_bridgeTestEvent = CreateEvent(NULL, TRUE, FALSE, NULL);

    // Read branch-local config.
    g_fyersTokenPath = BridgeDefaultTokenPath();
    g_bridgeScriptPath = BridgeDefaultScriptPath();

    CString cfg = BridgeModuleDir() + _T("\\FyersBridge.config");
    if (BridgeFileExists(cfg))
    {
        CStdioFile file;
        if (file.Open(cfg, CFile::modeRead | CFile::typeText))
        {
            CString line;
            while (file.ReadString(line))
            {
                line.Trim();
                int eq = line.Find(_T('='));
                if (eq <= 0)
                    continue;

                CString key = line.Left(eq);
                CString value = line.Mid(eq + 1);
                key.Trim();
                value.Trim();

                if (key.CompareNoCase(_T("AppId")) == 0)
                    g_fyersAppId = value;
                else if (key.CompareNoCase(_T("TokenFile")) == 0)
                    g_fyersTokenPath = BridgeResolvePath(value);
                else if (key.CompareNoCase(_T("BridgeScript")) == 0)
                    g_bridgeScriptPath = BridgeResolvePath(value);
            }
            file.Close();
        }
    }

    if (g_fyersAppId.IsEmpty() && !g_oServer.IsEmpty())
        g_fyersAppId = g_oServer;

    if (!g_bridgeWsaInit)
    {
        if (WSAStartup(MAKEWORD(2, 2), &g_bridgeWsaData) != 0)
        {
            BridgeStatus(_T("WSAStartup failed."));
            return FALSE;
        }
        g_bridgeWsaInit = TRUE;
    }

    InterlockedExchange(reinterpret_cast<volatile LONG*>(&g_bridgeStopping), 0);

    unsigned id = 0;
    g_bridgeConnectThread = (HANDLE)_beginthreadex(
        NULL, 0,
        [](void*) -> unsigned {
            BridgeConnectThreadProc(NULL);
            return 0;
        },
        NULL, 0, &id);

    unsigned wid = 0;
    g_bridgeWatchdogThread = (HANDLE)_beginthreadex(
        NULL, 0,
        [](void*) -> unsigned {
            BridgeWatchdogThreadProc(NULL);
            return 0;
        },
        NULL, 0, &wid);

    if (!g_bridgeConnectThread || !g_bridgeWatchdogThread)
    {
        BridgeStatus(_T("Could not start bridge monitoring threads."));
        return FALSE;
    }

    if (FyersBridgeCredentialsReady())
        BridgeLaunchPython();
    else
    {
        g_nStatus = STATUS_DISCONNECTED;
        BridgeStatus(_T("Configure the exact FYERS App ID and place access_token.txt beside FyersBridge.py."));
    }

    return TRUE;
}

void FyersDirectRelease()
{
    g_bridgeStopping = TRUE;

    BridgeSend(_T("SHUTDOWN"));
    BridgeCloseSocket();

    if (g_bridgeConnectThread)
    {
        WaitForSingleObject(g_bridgeConnectThread, 3000);
        CloseHandle(g_bridgeConnectThread);
        g_bridgeConnectThread = NULL;
    }

    if (g_bridgeWatchdogThread)
    {
        WaitForSingleObject(g_bridgeWatchdogThread, 3000);
        CloseHandle(g_bridgeWatchdogThread);
        g_bridgeWatchdogThread = NULL;
    }

    BridgeStopPython();

    if (g_bridgeWsaInit)
    {
        WSACleanup();
        g_bridgeWsaInit = FALSE;
    }

    if (g_bridgeStateCSInit)
    {
        EnterCriticalSection(&g_bridgeStateCS);
        POSITION pos = g_bridgeStates.GetStartPosition();
        while (pos != NULL)
        {
            CString key;
            BridgeSymbolState* state = NULL;
            g_bridgeStates.GetNextAssoc(pos, key, state);
            delete state;
        }
        g_bridgeStates.RemoveAll();
        LeaveCriticalSection(&g_bridgeStateCS);
    }

    if (g_bridgeRecentCSInit)
    {
        EnterCriticalSection(&g_bridgeRecentCS);
        POSITION pos = g_bridgeRecentMap.GetStartPosition();
        while (pos != NULL)
        {
            CString key;
            RecentInfo* info = NULL;
            g_bridgeRecentMap.GetNextAssoc(pos, key, info);
            delete info;
        }
        g_bridgeRecentMap.RemoveAll();
        LeaveCriticalSection(&g_bridgeRecentCS);
    }

    g_nStatus = STATUS_SHUTDOWN;
}

BOOL FyersDirectNotify(struct PluginNotification* pn)
{
    if (!pn)
        return FALSE;

    if (pn->nReason & REASON_DATABASE_LOADED)
    {
        g_hAmiBrokerWnd = pn->hMainWnd;

        EnterCriticalSection(&g_bridgeStateCS);
        POSITION pos = g_bridgeStates.GetStartPosition();
        while (pos != NULL)
        {
            CString key;
            BridgeSymbolState* state = NULL;
            g_bridgeStates.GetNextAssoc(pos, key, state);
            if (state)
            {
                state->fiveSecBars.RemoveAll();
                state->dailyBars.RemoveAll();
                state->currentBarStarted = FALSE;
                state->currentBarStart = 0;
                state->lastCompleted5s = 0;
                state->subscribed = FALSE;
                state->syncRequested = FALSE;
            }
        }
        LeaveCriticalSection(&g_bridgeStateCS);
    }

    if (pn->nReason & REASON_DATABASE_UNLOADED)
        g_hAmiBrokerWnd = NULL;

    if (pn->nReason & REASON_SETTINGS_CHANGE)
        FyersRequestReconnect();

    return TRUE;
}

RecentInfo* FyersDirectGetRecentInfo(LPCTSTR pszTicker)
{
    if (!pszTicker || g_nStatus == STATUS_SHUTDOWN)
        return NULL;

    BridgeSymbolState* state = BridgeState(pszTicker);
    if (!state)
        return NULL;

    if (!state->subscribed)
    {
        state->subscribed = TRUE;
        CString msg;
        msg.Format(_T("SUB\t%s"), (LPCTSTR)state->ticker);
        BridgeSend(msg);
    }

    return BridgeRecent(pszTicker);
}

BOOL FyersBridgeIsBackfillComplete(LPCTSTR pszTicker)
{
    BridgeSymbolState* state = BridgeState(pszTicker);
    if (!state)
        return FALSE;

    EnterCriticalSection(&g_bridgeStateCS);
    BOOL ready = state->fiveSecBars.GetCount() > 0 ||
                 state->dailyBars.GetCount() > 0;
    LeaveCriticalSection(&g_bridgeStateCS);
    return ready;
}

BOOL FyersDirectGetQuotesExInternal(LPCTSTR pszTicker,
                                    int nPeriodicity,
                                    int nLastValid,
                                    int nSize,
                                    struct Quotation* pQuotes)
{
    if (!pQuotes || nSize <= 0 ||
        !FyersBridgeCredentialsReady())
        return nLastValid >= 0 ? nLastValid + 1 : 0;

    if (nPeriodicity != 5 && nPeriodicity != 86400)
        return nLastValid >= 0 ? nLastValid + 1 : 0;

    BridgeSymbolState* state = BridgeState(pszTicker);
    if (!state)
        return nLastValid >= 0 ? nLastValid + 1 : 0;

    if (!state->subscribed)
    {
        state->subscribed = TRUE;
        CString msg;
        msg.Format(_T("SUB\t%s"), (LPCTSTR)state->ticker);
        BridgeSend(msg);
    }

    if (!state->syncRequested)
    {
        EnterCriticalSection(&g_bridgeStateCS);
        BOOL doSync = !state->syncRequested;
        state->syncRequested = TRUE;
        time_t last = state->lastCompleted5s;
        LeaveCriticalSection(&g_bridgeStateCS);

        if (doSync)
        {
            CString msg;
            msg.Format(_T("SYNC\t%s\t%lld"),
                       (LPCTSTR)state->ticker, (long long)last);
            BridgeSend(msg);
        }
    }

    return BridgeCopyData(state, nSize, pQuotes);
}

PLUGINAPI int GetPluginInfo(struct PluginInfo* pInfo)
{
    AFX_MANAGE_STATE(AfxGetStaticModuleState());
    if (!pInfo) return FALSE;
    *pInfo = oPluginInfo;
    return TRUE;
}

PLUGINAPI int Init(void)
{
    AFX_MANAGE_STATE(AfxGetStaticModuleState());
    return FyersDirectInit() ? 1 : 0;
}

PLUGINAPI int Release(void)
{
    AFX_MANAGE_STATE(AfxGetStaticModuleState());
    FyersDirectRelease();
    return 1;
}

PLUGINAPI int Configure(LPCTSTR pszPath, struct InfoSite* pSite)
{
    AFX_MANAGE_STATE(AfxGetStaticModuleState());
    UNREFERENCED_PARAMETER(pszPath);

    COpenAlgoConfigDlg dlg;
    dlg.m_pSite = pSite;

    if (dlg.DoModal() == IDOK && g_hAmiBrokerWnd)
        PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);

    return 1;
}

PLUGINAPI AmiVar GetExtraData(LPCTSTR, LPCTSTR, int, int, void* (*)(unsigned int))
{
    AmiVar v;
    v.type = VAR_NONE;
    v.val = 0;
    return v;
}

PLUGINAPI int SetTimeBase(int nTimeBase)
{
    return (nTimeBase == 5 || nTimeBase == 86400) ? 1 : 0;
}

PLUGINAPI int GetSymbolLimit(void)
{
    return 5000;
}

PLUGINAPI int GetPluginStatus(struct PluginStatus* s)
{
    if (!s) return 0;

    s->nStructSize = sizeof(*s);

    if (g_nStatus == STATUS_CONNECTED)
    {
        s->nStatusCode = 0;
        strcpy_s(s->szShortMessage, sizeof(s->szShortMessage), "OK");
        strcpy_s(s->szLongMessage, sizeof(s->szLongMessage), "FYERS Python bridge connected");
        s->clrStatusColor = RGB(0,255,0);
    }
    else if (g_nStatus == STATUS_WAIT)
    {
        s->nStatusCode = 0x10000000;
        strcpy_s(s->szShortMessage, sizeof(s->szShortMessage), "WAIT");
        strcpy_s(s->szLongMessage, sizeof(s->szLongMessage), "FYERS Python bridge is starting");
        s->clrStatusColor = RGB(255,255,0);
    }
    else if (g_nStatus == STATUS_SHUTDOWN)
    {
        s->nStatusCode = 0x30000000;
        strcpy_s(s->szShortMessage, sizeof(s->szShortMessage), "OFF");
        strcpy_s(s->szLongMessage, sizeof(s->szLongMessage), "FYERS Python bridge stopped");
        s->clrStatusColor = RGB(192,0,192);
    }
    else
    {
        s->nStatusCode = 0x20000000;
        strcpy_s(s->szShortMessage, sizeof(s->szShortMessage), "ERR");
        strcpy_s(s->szLongMessage, sizeof(s->szLongMessage),
                 "FYERS Python bridge disconnected; automatic retry is active");
        s->clrStatusColor = RGB(255,0,0);
    }

    return 1;
}

PLUGINAPI int GetStatus(struct PluginStatus* s)
{
    return GetPluginStatus(s);
}

PLUGINAPI int SetDatabasePath(LPCTSTR)
{
    return 1;
}

PLUGINAPI struct RecentInfo* GetRecentInfo(LPCTSTR pszTicker)
{
    AFX_MANAGE_STATE(AfxGetStaticModuleState());
    return FyersDirectGetRecentInfo(pszTicker);
}

PLUGINAPI BOOL IsBackfillComplete(LPCTSTR pszTicker)
{
    AFX_MANAGE_STATE(AfxGetStaticModuleState());
    return FyersBridgeIsBackfillComplete(pszTicker);
}

PLUGINAPI int GetQuotesEx(LPCTSTR pszTicker, int nPeriodicity,
                          int nLastValid, int nSize,
                          struct Quotation* pQuotes,
                          GQEContext*)
{
    AFX_MANAGE_STATE(AfxGetStaticModuleState());
    return FyersDirectGetQuotesExInternal(
        pszTicker, nPeriodicity, nLastValid, nSize, pQuotes);
}

PLUGINAPI int GetQuotes(LPCTSTR pszTicker, int nPeriodicity,
                        int nLastValid, int nSize,
                        struct QuotationFormat4* pQuotes4)
{
    if (!pQuotes4 || nSize <= 0)
        return nLastValid >= 0 ? nLastValid + 1 : 0;

    Quotation* temp = (Quotation*)malloc(
        (size_t)nSize * sizeof(Quotation));
    if (!temp)
        return nLastValid >= 0 ? nLastValid + 1 : 0;

    int seed = min(nSize, max(0, nLastValid + 1));
    for (int i = 0; i < seed; ++i)
        ConvertFormat4Quote(&pQuotes4[i], &temp[i]);

    int n = FyersDirectGetQuotesExInternal(
        pszTicker, nPeriodicity, nLastValid, nSize, temp);

    for (int i = 0; i < n && i < nSize; ++i)
        ConvertFormat5Quote(&temp[i], &pQuotes4[i]);

    free(temp);
    return n;
}

PLUGINAPI int Notify(struct PluginNotification* pn)
{
    AFX_MANAGE_STATE(AfxGetStaticModuleState());
    if (!pn) return 0;
    return FyersDirectNotify(pn) ? 1 : 0;
}
