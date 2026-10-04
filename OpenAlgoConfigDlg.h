#ifndef OPENALGOCONFIGDLG_H
#define OPENALGOCONFIGDLG_H
#pragma once
#include "resource.h"

class COpenAlgoConfigDlg : public CDialog
{
public:
    COpenAlgoConfigDlg(CWnd* pParent = NULL);
    enum { IDD = IDD_CONFIG_DIALOG };
    struct InfoSite* m_pSite;

protected:
    virtual void DoDataExchange(CDataExchange* pDX);
    virtual BOOL OnInitDialog();
    virtual void OnOK();
    afx_msg void OnTestConnectionButton();
    afx_msg void OnTestWebSocketButton();
    DECLARE_MESSAGE_MAP()
};

#endif
