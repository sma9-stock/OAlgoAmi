// OpenAlgoConfigDlg.cpp : implementation file
#include "stdafx.h"
#include "resource.h"  // Include resource definitions
#include "Plugin.h"  // Include Plugin.h before OpenAlgoGlobals.h
#include "OpenAlgoGlobals.h"  // Include global variables
#include "OpenAlgoPlugin.h"
#include "OpenAlgoConfigDlg.h"

#ifdef _DEBUG
#define new DEBUG_NEW
#undef THIS_FILE
static char THIS_FILE[] = __FILE__;
#endif

// Global variables are now properly declared in OpenAlgoGlobals.h
// No need for extern declarations here

/////////////////////////////////////////////////////////////////////////////
// COpenAlgoConfigDlg dialog

COpenAlgoConfigDlg::COpenAlgoConfigDlg(CWnd* pParent /*=NULL*/)
	: CDialog(COpenAlgoConfigDlg::IDD, pParent)
{
	//{{AFX_DATA_INIT(COpenAlgoConfigDlg)
	//}}AFX_DATA_INIT

	// Initialize m_pSite member variable
	m_pSite = NULL;
}

void COpenAlgoConfigDlg::DoDataExchange(CDataExchange* pDX)
{
	CDialog::DoDataExchange(pDX);
	//{{AFX_DATA_MAP(COpenAlgoConfigDlg)
	//}}AFX_DATA_MAP

	// Map dialog controls to global variables
	DDX_Text(pDX, IDC_SERVER_EDIT, g_oServer);
	DDV_MaxChars(pDX, g_oServer, 255);

	DDX_Text(pDX, IDC_APIKEY_EDIT, g_oApiKey);
	DDV_MaxChars(pDX, g_oApiKey, 255);

	DDX_Text(pDX, IDC_PORT_EDIT, g_nPortNumber);
	DDV_MinMaxInt(pDX, g_nPortNumber, 1, 65535);

	DDX_Text(pDX, IDC_INTERVAL_EDIT, g_nBackfillRefreshIntervalSec);
	DDV_MinMaxInt(pDX, g_nBackfillRefreshIntervalSec, 5, 3600); // 5 seconds to 1 hour

	DDX_Text(pDX, IDC_TIMESHIFT_EDIT, g_nTimeShift);
	DDV_MinMaxInt(pDX, g_nTimeShift, -48, 48);
	
	DDX_Text(pDX, IDC_WEBSOCKET_EDIT, g_oWebSocketUrl);
	DDV_MaxChars(pDX, g_oWebSocketUrl, 255);
}

BEGIN_MESSAGE_MAP(COpenAlgoConfigDlg, CDialog)
	//{{AFX_MSG_MAP(COpenAlgoConfigDlg)
	ON_BN_CLICKED(IDC_TEST_CONNECTION_BUTTON, OnTestConnectionButton)
	ON_BN_CLICKED(IDC_TEST_WEBSOCKET_BUTTON, OnTestWebSocketButton)
	ON_EN_SETFOCUS(IDC_APIKEY_EDIT, OnApiKeyEditSetFocus)
	//}}AFX_MSG_MAP
END_MESSAGE_MAP()

// Log helper: never spell out the full API key. Keep the first/last few chars
// only so the user can tell from DbgView which key got saved without leaking it.
static CString MaskKey(const CString& key)
{
	CString s;
	int n = key.GetLength();
	if (n <= 8) { s.Format(_T("(len=%d)"), n); return s; }
	s.Format(_T("%s...%s (len=%d)"), (LPCTSTR)key.Left(4), (LPCTSTR)key.Right(4), n);
	return s;
}

/////////////////////////////////////////////////////////////////////////////
// COpenAlgoConfigDlg message handlers

BOOL COpenAlgoConfigDlg::OnInitDialog()
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());
	CDialog::OnInitDialog();

	SetWindowText(_T("OpenAlgo Plugin Configuration"));

	// Diagnostic so we can verify in DbgView which key the dialog opened with.
	CString log;
	log.Format(_T("OpenAlgo: Config dialog opened. Existing ApiKey=%s"), (LPCTSTR)MaskKey(g_oApiKey));
	OutputDebugString(log);

	CWnd* pServerEdit = GetDlgItem(IDC_SERVER_EDIT);
	if (pServerEdit)
		pServerEdit->SetFocus();

	return FALSE;
}

// Auto-select all text in the API Key field whenever it receives focus so
// users replace the existing masked key by typing rather than appending to it.
void COpenAlgoConfigDlg::OnApiKeyEditSetFocus()
{
	CEdit* pEdit = (CEdit*)GetDlgItem(IDC_APIKEY_EDIT);
	if (pEdit)
		pEdit->SetSel(0, -1);
}

void COpenAlgoConfigDlg::OnOK()
{
	AFX_MANAGE_STATE(AfxGetStaticModuleState());
	if (!UpdateData(TRUE)) return;

	CString appId = g_oServer; appId.Trim();
	CString token = g_oApiKey; token.Trim();

	if (appId.IsEmpty())
	{
		AfxMessageBox(_T("Please enter the FYERS App ID."), MB_OK | MB_ICONWARNING);
		GetDlgItem(IDC_SERVER_EDIT)->SetFocus();
		return;
	}
	if (token.IsEmpty())
	{
		AfxMessageBox(_T("Please enter the FYERS Access Token."), MB_OK | MB_ICONWARNING);
		GetDlgItem(IDC_APIKEY_EDIT)->SetFocus();
		return;
	}

	if (!FyersDirectReconfigure(appId, token))
	{
		AfxMessageBox(_T("Could not save FYERS credentials. The Access Token is protected with Windows DPAPI."), MB_OK | MB_ICONERROR);
		return;
	}

	g_oServer = appId;
	g_oApiKey = token;
	g_nStatus = STATUS_WAIT;
	if (g_hAmiBrokerWnd)
		PostMessage(g_hAmiBrokerWnd, WM_USER_STREAMING_UPDATE, 0, 0);
	CDialog::OnOK();
}

void COpenAlgoConfigDlg::OnTestConnectionButton()
{
	if (!UpdateData(TRUE)) return;
	SetDlgItemText(IDC_STATUS_STATIC, _T("Testing FYERS REST connection..."));
	BOOL ok = FyersDirectReconfigure(g_oServer, g_oApiKey) && FyersTestRestConnection();
	SetDlgItemText(IDC_STATUS_STATIC, ok ? _T("FYERS REST connection successful.")
	                                    : _T("FYERS REST connection failed. Check App ID, Access Token and network."));
}

void COpenAlgoConfigDlg::OnTestWebSocketButton()
{
	if (!UpdateData(TRUE)) return;
	SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC, _T("Testing FYERS WebSocket..."));
	BOOL ok = FyersDirectReconfigure(g_oServer, g_oApiKey) && FyersTestWebSocket();
	SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC, ok ? _T("FYERS WebSocket connected.")
	                                               : _T("FYERS WebSocket connection failed."));
}


void COpenAlgoConfigDlg::OnTestWebSocketButton()
{
	// Update data from controls
	UpdateData(TRUE);

	// Validate WebSocket URL
	if (g_oWebSocketUrl.IsEmpty())
	{
		SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC, _T("Please enter a WebSocket URL"));
		return;
	}

	// Validate API Key
	if (g_oApiKey.IsEmpty())
	{
		SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC, _T("API Key is required"));
		return;
	}

	// Change cursor to wait cursor
	CWaitCursor wait;

	SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC, _T("Testing..."));

	// Test WebSocket connection
	if (TestWebSocketConnection(g_oWebSocketUrl, g_oApiKey))
	{
		// Success message is set by TestWebSocketConnection function
	}
	else
	{
		SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC,
			_T("WebSocket connection failed"));
	}
}

BOOL COpenAlgoConfigDlg::TestWebSocketConnection(const CString& wsUrl, const CString& apiKey)
{
	BOOL bSuccess = FALSE;
	SOCKET sock = INVALID_SOCKET;
	
	try
	{
		// Initialize Winsock
		WSADATA wsaData;
		if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
		{
			return FALSE;
		}

		// Parse WebSocket URL
		CString host, path;
		int port = 80;
		BOOL bSecure = FALSE;

		CString url = wsUrl;
		if (url.Left(5) == _T("wss://"))
		{
			bSecure = TRUE;
			port = 443;
			url = url.Mid(6);
		}
		else if (url.Left(5) == _T("ws://"))
		{
			url = url.Mid(5);
		}

		// Extract host and port
		int slashPos = url.Find(_T('/'));
		if (slashPos > 0)
		{
			host = url.Left(slashPos);
			path = url.Mid(slashPos);
		}
		else
		{
			host = url;
			path = _T("/");
		}

		int colonPos = host.Find(_T(':'));
		if (colonPos > 0)
		{
			CString portStr = host.Mid(colonPos + 1);
			port = _ttoi(portStr);
			host = host.Left(colonPos);
		}

		// For simplicity, we'll just test TCP connection here
		// Full WebSocket implementation would require HTTP upgrade and WebSocket framing
		
		// Create socket
		sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
		if (sock == INVALID_SOCKET)
		{
			WSACleanup();
			return FALSE;
		}

		// Set timeout
		int timeout = 5000; // 5 seconds
		setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout));
		setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (char*)&timeout, sizeof(timeout));

		// Resolve hostname
		struct addrinfo hints, *result;
		ZeroMemory(&hints, sizeof(hints));
		hints.ai_family = AF_INET;
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_protocol = IPPROTO_TCP;

		CStringA hostA(host);
		CStringA portStrA;
		portStrA.Format("%d", port);

		if (getaddrinfo(hostA, portStrA, &hints, &result) != 0)
		{
			closesocket(sock);
			WSACleanup();
			return FALSE;
		}

		// Connect to server
		if (connect(sock, result->ai_addr, (int)result->ai_addrlen) == SOCKET_ERROR)
		{
			freeaddrinfo(result);
			closesocket(sock);
			WSACleanup();
			return FALSE;
		}

		freeaddrinfo(result);

		// Send WebSocket upgrade request
		CString upgradeRequest;
		upgradeRequest.Format(
			_T("GET %s HTTP/1.1\r\n")
			_T("Host: %s:%d\r\n")
			_T("Upgrade: websocket\r\n")
			_T("Connection: Upgrade\r\n")
			_T("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n")
			_T("Sec-WebSocket-Version: 13\r\n")
			_T("\r\n"),
			(LPCTSTR)path, (LPCTSTR)host, port);

		CStringA requestA(upgradeRequest);
		if (send(sock, requestA, requestA.GetLength(), 0) == SOCKET_ERROR)
		{
			closesocket(sock);
			WSACleanup();
			return FALSE;
		}

		// Receive response
		char buffer[1024];
		int received = recv(sock, buffer, sizeof(buffer) - 1, 0);
		if (received > 0)
		{
			buffer[received] = '\0';
			CString response(buffer);
			
			// Check for successful upgrade
			if (response.Find(_T("101")) > 0 && response.Find(_T("Switching Protocols")) > 0)
			{
				// Now send authentication message
				CString authMsg = _T("{\"action\":\"authenticate\",\"api_key\":\"") + apiKey + _T("\"}");

				if (SendWebSocketFrame(sock, authMsg))
				{
					// Wait for authentication response
					char authBuffer[1024];
					int authReceived = recv(sock, authBuffer, sizeof(authBuffer) - 1, 0);
					if (authReceived > 0)
					{
						CString authResponse = DecodeWebSocketFrame(authBuffer, authReceived);
						
						// Check for success status in authentication response
						if (authResponse.Find(_T("success")) >= 0 ||
							authResponse.Find(_T("authenticated")) >= 0 ||
							authResponse.Find(_T("\"status\":\"ok\"")) >= 0)
						{
							// Authentication successful, now test subscribe to RELIANCE-NSE
							CString subMsg = _T("{\"action\":\"subscribe\",\"symbol\":\"RELIANCE\",\"exchange\":\"NSE\",\"mode\":2}");

							if (SendWebSocketFrame(sock, subMsg))
							{
								// Wait for subscription response
								Sleep(1000);

								// Try to receive any response (subscription confirmation or data)
								char quoteBuffer[2048];
								int quoteReceived = recv(sock, quoteBuffer, sizeof(quoteBuffer) - 1, 0);

								BOOL subscriptionWorked = FALSE;
								if (quoteReceived > 0)
								{
									CString response = DecodeWebSocketFrame(quoteBuffer, quoteReceived);

									// Check if we got any valid response (subscription confirmation or market data)
									if (!response.IsEmpty())
									{
										subscriptionWorked = TRUE;
									}
								}

								// Unsubscribe
								CString unsubMsg = _T("{\"action\":\"unsubscribe\",\"symbol\":\"RELIANCE\",\"exchange\":\"NSE\",\"mode\":2}");
								SendWebSocketFrame(sock, unsubMsg);

								// Show simple success message
								if (subscriptionWorked)
								{
									SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC,
										_T("WebSocket connection successful!"));
								}
								else
								{
									SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC,
										_T("WebSocket connection successful (authenticated)"));
								}

								bSuccess = TRUE;
							}
							else
							{
								SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC,
									_T("Connected but subscription test failed"));
							}
						}
						else
						{
							SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC,
								_T("WebSocket connection failed: Authentication rejected"));
						}
					}
					else
					{
						SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC,
							_T("WebSocket connection failed: No auth response"));
					}
				}
				else
				{
					SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC,
						_T("WebSocket connection failed: Auth send failed"));
				}
			}
			else
			{
				SetDlgItemText(IDC_WEBSOCKET_STATUS_STATIC,
					_T("WebSocket connection failed: Upgrade failed"));
			}
		}

		closesocket(sock);
		WSACleanup();
	}
	catch (...)
	{
		if (sock != INVALID_SOCKET)
		{
			closesocket(sock);
		}
		WSACleanup();
		return FALSE;
	}

	return bSuccess;
}

void COpenAlgoConfigDlg::GenerateMaskKey(unsigned char* maskKey)
{
	// Generate a simple random mask key
	srand((unsigned int)GetTickCount64());
	maskKey[0] = (unsigned char)(rand() & 0xFF);
	maskKey[1] = (unsigned char)(rand() & 0xFF);
	maskKey[2] = (unsigned char)(rand() & 0xFF);
	maskKey[3] = (unsigned char)(rand() & 0xFF);
}

BOOL COpenAlgoConfigDlg::SendWebSocketFrame(SOCKET sock, const CString& message)
{
	// Convert message to UTF-8
	CStringA messageA(message);
	int messageLen = messageA.GetLength();
	
	// Create WebSocket frame
	unsigned char frame[1024];
	int frameLen = 0;
	
	// First byte: FIN=1, OpCode=1 (text frame)
	frame[frameLen++] = 0x81;
	
	// Second byte: MASK=1 + Payload length
	if (messageLen < 126)
	{
		frame[frameLen++] = 0x80 | messageLen; // MASK=1 + length
	}
	else if (messageLen < 65536)
	{
		frame[frameLen++] = 0x80 | 126; // MASK=1 + extended length indicator
		frame[frameLen++] = (messageLen >> 8) & 0xFF;
		frame[frameLen++] = messageLen & 0xFF;
	}
	else
	{
		return FALSE; // Message too long
	}
	
	// Generate masking key
	unsigned char maskKey[4];
	GenerateMaskKey(maskKey);
	memcpy(&frame[frameLen], maskKey, 4);
	frameLen += 4;
	
	// Masked payload
	for (int i = 0; i < messageLen; i++)
	{
		frame[frameLen++] = messageA[i] ^ maskKey[i % 4];
	}
	
	// Send the frame
	int sent = send(sock, (char*)frame, frameLen, 0);
	return (sent == frameLen);
}

CString COpenAlgoConfigDlg::DecodeWebSocketFrame(const char* buffer, int length)
{
	CString result;
	
	if (length < 2) return result;
	
	int pos = 0;
	unsigned char firstByte = (unsigned char)buffer[pos++];
	unsigned char secondByte = (unsigned char)buffer[pos++];
	
	// Check if this is a text frame
	if ((firstByte & 0x0F) != 0x01) return result; // Not a text frame
	
	BOOL masked = (secondByte & 0x80) != 0;
	int payloadLen = secondByte & 0x7F;
	
	// Handle extended payload length
	if (payloadLen == 126)
	{
		if (pos + 2 > length) return result;
		payloadLen = ((unsigned char)buffer[pos] << 8) | (unsigned char)buffer[pos + 1];
		pos += 2;
	}
	else if (payloadLen == 127)
	{
		// 64-bit length not supported
		return result;
	}
	
	// Handle masking key
	unsigned char maskKey[4] = {0};
	if (masked)
	{
		if (pos + 4 > length) return result;
		memcpy(maskKey, &buffer[pos], 4);
		pos += 4;
	}
	
	// Extract and unmask payload
	if (pos + payloadLen <= length)
	{
		CStringA payloadA;
		char* payloadBuffer = payloadA.GetBuffer(payloadLen + 1);
		
		for (int i = 0; i < payloadLen; i++)
		{
			if (masked)
			{
				payloadBuffer[i] = buffer[pos + i] ^ maskKey[i % 4];
			}
			else
			{
				payloadBuffer[i] = buffer[pos + i];
			}
		}
		payloadBuffer[payloadLen] = '\0';
		payloadA.ReleaseBuffer(payloadLen);
		
		result = CString(payloadA);
	}
	
	return result;
}

