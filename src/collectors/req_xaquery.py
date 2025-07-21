import os
import win32com.client
import pythoncom
import threading
import pickle
from datetime import datetime, timedelta, time
import telegram
import math
import numpy as np
from src.collectors.xaquery import XAQuery
from dotenv import load_dotenv
from src.api.xasession import XASession


class ReqXAQuery:
    def __init__(self):
        # .env 파일 로드
        load_dotenv()

        # 기본 설정 로드
        self.user_id = os.getenv('LS_USER_ID')
        self.password = os.getenv('LS_PASSWORD')
        self.cert_password = os.getenv('LS_CERT_PASSWORD')
        self.account_type = os.getenv('ACCOUNT_TYPE', 'demo')  # 기본값: demo
        self.api_port = int(os.getenv('API_PORT', 20001))  # 기본값: 20001

        # 계좌 타입에 따른 설정
        if self.account_type == 'real':
            self.server_address = 'api.ls-sec.co.kr'
            self.account_number = os.getenv('REAL_ACCOUNT_NUMBER')
            self.account_password = os.getenv('REAL_ACCOUNT_PASSWORD')
        else:  # demo
            self.server_address = 'demo.ls-sec.co.kr'
            self.account_number = os.getenv('DEMO_ACCOUNT_NUMBER')
            self.account_password = os.getenv('DEMO_ACCOUNT_PASSWORD')

        ##########로그인부분#####################
        self.session = win32com.client.DispatchWithEvents("XA_Session.XASession", XASession)
        self.session.ConnectServer(self.server_address, 20001)
        self.session.Login(self.user_id, self.password, self.cert_password, 0, False)

        while XASession.login_ok is False:
            pythoncom.PumpWaitingMessages()

    # 예수금 가져오기
    def acc(self):
        XAQuery.CSPAQ12200_event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.CSPAQ12200_event.ResFileName = "C:/eBEST/xingAPI/Res/CSPAQ12200.res"
        count = self.session.GetAccountListCount()
        for i in range(count):
            szAcct = self.session.GetAccountList(i)
            if szAcct == self.account_number:
                XASession.deposit(acc_no=szAcct)


if __name__ == "__main__":
    # SessionManager 테스트
    reqxaquery = ReqXAQuery()

