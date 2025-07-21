import time
import win32com.client
import pythoncom
from src.collectors.xaquery import XAQuery


# 서버접속 및 로그인 요청 이후 수신결과 데이터를 다르는 구간
class XASession:
    login_ok = False
    CSPAQ12200_event = None
    CSPAQ12200_ok = False

    def OnLogin(self, szCode, szMsg):
        print("%s %s" % (szCode, szMsg), flush=True)

        if szCode == "0000":
            XASession.login_ok = True
            self.login_ok = True
        else:
            XASession.login_ok = False

    def deposit(self, acc_no=None, acc_no_pwd="0000"):

        time.sleep(5.1)

        XAQuery.CSPAQ12200_event.SetFieldData("CSPAQ12200InBlock1", "RecCnt", 0, 1)
        XAQuery.CSPAQ12200_event.SetFieldData("CSPAQ12200InBlock1", "MgmtBrnNo", 0, "")
        XAQuery.CSPAQ12200_event.SetFieldData("CSPAQ12200InBlock1", "AcntNo", 0, acc_no)
        XAQuery.CSPAQ12200_event.SetFieldData("CSPAQ12200InBlock1", "Pwd", 0, acc_no_pwd)
        XAQuery.CSPAQ12200_event.SetFieldData("CSPAQ12200InBlock1", "BalCreTp", 0, "0")
        XAQuery.CSPAQ12200_event.Request(False)

        XAQuery.CSPAQ12200_ok = False
        while XAQuery.CSPAQ12200_ok is False:
            pythoncom.PumpWaitingMessages()

    def price_inquiry_by_theme_item(tmcode=None):
        XAQuery.t1537_event.SetFieldData("t1537InBlock", "tmcode", 0, tmcode)
        err = XAQuery.t1537_event.Request(False)

        if err < 0:
            print("테마종목별시세조회 에러")
            # Main.send_msg_telegram("테마종목별시세조회 에러")