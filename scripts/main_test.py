import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.collectors.req_xaquery import ReqXAQuery


class Main:
    def __init__(self):
        print("Main 클래스 초기화됨")


if __name__ == "__main__":
    print("메인 실행")
    ReqXAQuery()
