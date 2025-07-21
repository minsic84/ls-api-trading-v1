import os
import win32com.client
import pythoncom
from dotenv import load_dotenv
from src.api.xasession import XASession
from src.collectors.req_xaquery import ReqXAQuery

class Main():
    def __init__(self):
        print("클래스 실행")

if __name__ == "__main__":
    # SessionManager 테스트
    reqxaquery = ReqXAQuery()
    reqxaquery.acc()

