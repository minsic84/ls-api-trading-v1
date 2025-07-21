import os
import time
import pythoncom
import win32com.client
from dotenv import load_dotenv
from src.collectors.xaquery import XAQuery
from src.api.xasession import XASession


class ReqXAQuery:
    def __init__(self):
        load_dotenv()

        self.user_id = os.getenv('LS_USER_ID')
        self.password = os.getenv('LS_PASSWORD')
        self.cert_password = os.getenv('LS_CERT_PASSWORD')
        self.account_type = os.getenv('ACCOUNT_TYPE', 'demo')
        self.server_address = 'demo.ls-sec.co.kr'

        self.session = win32com.client.DispatchWithEvents("XA_Session.XASession", XASession)
        self.session.ConnectServer(self.server_address, 20001)
        self.session.Login(self.user_id, self.password, self.cert_password, 0, False)

        while XASession.login_ok is False:
            pythoncom.PumpWaitingMessages()

        self.init_events()

    def init_events(self):
        XAQuery.t8425_event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.t8425_event.ResFileName = "C:/LS_SEC/xingAPI/Res/t8425.res"
        self.view_all_themems(dummy=None)
        time.sleep(1)

        # for tmcode in list(XAQuery.t8425_dict.keys())[:5]:  # 일부만 테스트
        #     XAQuery.t1537_event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        #     XAQuery.t1537_event.ResFileName = "C:/LS_SEC/xingAPI/Res/t1537.res"
        #     XAQuery.t1537_event.SetFieldData("t1537InBlock", "tmcode", 0, tmcode)
        #     XAQuery.t1537_event.Request(False)
        #     pythoncom.PumpWaitingMessages()

    def view_all_themems(self, dummy=None):
        XAQuery.t8425_event.SetFieldData("t8425InBlock", "dummy", 0, dummy)
        err = XAQuery.t8425_event.Request(False)

        if err < 0:
            print("테마조회요청 에러")
            # Main.send_msg_telegram("테마조회요청 에러")
