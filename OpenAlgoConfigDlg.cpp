#include "stdafx.h"
#include "resource.h"
#include "Plugin.h"
#include "OpenAlgoGlobals.h"
#include "OpenAlgoPlugin.h"
#include "OpenAlgoConfigDlg.h"

COpenAlgoConfigDlg::COpenAlgoConfigDlg(CWnd* pParent)
    : CDialog(COpenAlgoConfigDlg::IDD, pParent), m_pSite(NULL)
{
}

void COpenAlgoConfigDlg::DoDataExchange(CDataExchange* pDX)
{
    CDialog::DoDataExchange(pDX);
    DDX_Text(pDX, IDC_SERVER_EDIT, g_oServer);
    DDV_MaxChars(pDX, g_oServer, 255);
    DDX_Text(pDX, IDC_APIKEY_EDIT, g_fyersTokenFilePath);
    DDV_MaxChars(pDX, g_fyersTokenFilePath, 1023);
    DDX_Text(pDX, IDC_WEBSOCKET_EDIT, g_fyersBridgeScriptPath);
    DDV_MaxChars(pDX, g_fyersBridgeScriptPath, 1023);
    DDX_Text(pDX, IDC_INTERVAL_EDIT, g_fyersGapCheckIntervalSec);
    DDV_MinMaxInt(pDX, g_fyersGapCheckIntervalSec, 1, 3600);
}

BEGIN_MESSAGE_MAP(COpenAlgoConfigDlg, CDialog)
    ON_BN_CLICKED(IDC_TEST_CONNECTION_BUTTON, OnTestConnectionButton)
    ON_BN_CLICKED(IDC_TEST_WEBSOCKET_BUTTON, OnTestWebSocketButton)
END_MESSAGE_MAP()

static CString MaskCredential(const CString& s)
{
    CString out;
    if (s.GetLength() <= 8)
    {
        out.Format(_T("(len=%d)"), s.GetLength());
        return out;
    }
    out.Format(_T("%s...%s (len=%d)"), (LPCTSTR)s.Left(4),
               (LPCTSTR)s.Right(4), s.GetLength());
    return out;
}

BOOL COpenAlgoConfigDlg::OnInitDialog()
{
    AFX_MANAGE_STATE(AfxGetStaticModuleState());
    CDialog::OnInitDialog();

    SetWindowText(_T("FYERS Direct - 5-Second Data"));
    g_oServer = g_fyersAppId;
    if (g_fyersTokenFilePath.IsEmpty()) g_fyersTokenFilePath = _T("access_token.txt");
    if (g_fyersBridgeScriptPath.IsEmpty()) g_fyersBridgeScriptPath = _T("FyersBridge.py");

    CString status;
    status.Format(_T("FYERS App ID: %s"), (LPCTSTR)MaskCredential(g_oServer));
    SetDlgItemText(IDC_STATUS_STATIC, status);
    SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC,
                   _T("Official FYERS Python DataSocket + local bridge"));
    return TRUE;
}

void COpenAlgoConfigDlg::OnOK()
{
    AFX_MANAGE_STATE(AfxGetStaticModuleState());
    if (!UpdateData(TRUE))
        return;

    CString appId = g_oServer;
    CString tokenFile = g_fyersTokenFilePath;
    CString bridgeScript = g_fyersBridgeScriptPath;
    appId.Trim();
    tokenFile.Trim();
    bridgeScript.Trim();

    if (appId.IsEmpty())
    {
        AfxMessageBox(_T("Enter the FYERS App ID."));
        return;
    }

    if (tokenFile.IsEmpty())
        tokenFile = _T("access_token.txt");

    if (!FyersDirectReconfigure(appId, tokenFile, bridgeScript))
    {
        AfxMessageBox(_T("Could not save FYERS credentials."));
        return;
    }

    g_nStatus = STATUS_WAIT;
    if (g_hAmiBrokerWnd)
        ::PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);

    CDialog::OnOK();
}

void COpenAlgoConfigDlg::OnTestConnectionButton()
{
    if (!UpdateData(TRUE))
        return;

    CString appId = g_oServer;
    CString tokenFile = g_fyersTokenFilePath;
    CString bridgeScript = g_fyersBridgeScriptPath;
    appId.Trim();
    tokenFile.Trim();
    bridgeScript.Trim();

    if (appId.IsEmpty() || tokenFile.IsEmpty())
    {
        SetDlgItemText(IDC_STATUS_STATIC,
                       _T("App ID and access_token.txt path are required."));
        return;
    }

    SetDlgItemText(IDC_STATUS_STATIC, _T("Testing FYERS REST..."));

    if (!FyersDirectReconfigure(appId, tokenFile, bridgeScript) ||
        !FyersTestRestConnection())
    {
        SetDlgItemText(IDC_STATUS_STATIC,
                       g_fyersLastError.IsEmpty() ? _T("FYERS REST test failed.") : g_fyersLastError);
        return;
    }

    SetDlgItemText(IDC_STATUS_STATIC, _T("FYERS REST test passed."));
}

void COpenAlgoConfigDlg::OnTestWebSocketButton()
{
    if (!UpdateData(TRUE))
        return;

    CString appId = g_oServer;
    CString tokenFile = g_fyersTokenFilePath;
    CString bridgeScript = g_fyersBridgeScriptPath;
    appId.Trim();
    tokenFile.Trim();
    bridgeScript.Trim();

    if (appId.IsEmpty() || tokenFile.IsEmpty())
    {
        SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC,
                       _T("App ID and token file are required."));
        return;
    }

    SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC,
                   _T("Testing FYERS WebSocket..."));

    if (!FyersDirectReconfigure(appId, tokenFile, bridgeScript) ||
        !FyersTestWebSocket())
    {
        SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC,
                       _T("FYERS WebSocket test failed."));
        return;
    }

    SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC,
                   _T("FYERS WebSocket test passed."));
}
