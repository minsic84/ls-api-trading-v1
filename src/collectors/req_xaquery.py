import os
import time
import pythoncom
import self
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

        print("🔐 로그인 시작...")
        self.session = win32com.client.DispatchWithEvents("XA_Session.XASession", XASession)
        self.session.ConnectServer(self.server_address, 20001)
        self.session.Login(self.user_id, self.password, self.cert_password, 0, False)

        # 로그인 대기
        while XASession.login_ok is False:
            pythoncom.PumpWaitingMessages()

        print("✅ 로그인 성공!")
        self.run_stock_themes()


    def run_stock_themes(self):
        self.init_events()
        for theme in XAQuery.t8425_dict.keys():
            self.price_inquiry_by_theme_item(theme)
            time.sleep(3.1)
        self.run_search_stock(upcode='001')

    def init_events(self):
        # t8425 이벤트 초기화
        XAQuery.t8425_event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.t8425_event.ResFileName = "C:/eBEST/xingAPI/Res/t8425.res"

        # t8425 테마전체조회 실행
        print("📊 t8425 테마전체조회 시작...")
        self.view_all_themes()

    def view_all_themes(self, dummy=None):
        """테마전체조회 실행 및 응답 대기"""

        # 🚨 중요! 완료 플래그 초기화
        XAQuery.t8425_ok = False

        # 요청 실행
        XAQuery.t8425_event.SetFieldData("t8425InBlock", "dummy", 0, dummy if dummy else "")
        err = XAQuery.t8425_event.Request(False)

        if err < 0:
            print(f"❌ t8425 요청 실패: {err}")
            return False

        print("⏳ t8425 응답 대기 중...")

        # 🚨 핵심! 응답이 올 때까지 대기
        timeout = 0
        while XAQuery.t8425_ok is False and timeout < 100:  # 10초 타임아웃
            pythoncom.PumpWaitingMessages()
            time.sleep(0.1)
            timeout += 1

        if timeout >= 100:
            print("❌ t8425 응답 타임아웃")
            return False

        print(f"🎉 t8425 완료! 총 {len(XAQuery.t8425_dict)}개 테마 수신")

        # 결과 출력
        if XAQuery.t8425_dict:
            print("\n📋 수신된 테마 목록:")
            for i, (tmcode, data) in enumerate(XAQuery.t8425_dict.items(), 1):
                print(f"{i:3d}. {data['테마이름']} ({tmcode})")

        return True

    def price_inquiry_by_theme_item(self, tmcode=None):
        XAQuery.t1537_event.SetFieldData("t1537InBlock", "tmcode", 0, tmcode)
        err = XAQuery.t1537_event.Request(False)

        if err < 0:
            print("테마종목별시세조회 에러")

    def run_search_stock(self, upcode=None):
        self.search_stock(upcode=upcode, shcode="", IsNext=False)

    def search_stock(self, upcode=None, shcode=None, IsNext=False):

        time.sleep(3.1)

        XAQuery.t1516_event.SetFieldData("t1516InBlock", "upcode", 0, upcode)  # 업종코드
        XAQuery.t1516_event.SetFieldData("t1516InBlock", "gubun", 0, '')  # 구분
        XAQuery.t1516_event.SetFieldData("t1516InBlock", "shcode", 0, shcode)  # 종목코드
        XAQuery.t1516_event.Request(IsNext)

        XAQuery.t1516_ok = False
        while XAQuery.t1516_ok is False:
            pythoncom.PumpWaitingMessages()


if __name__ == "__main__":
    print("🚀 LS API Trading System 시작")
    try:
        req_query = ReqXAQuery()
        print("✅ 시스템 실행 완료")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")