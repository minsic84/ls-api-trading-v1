import os
import win32com.client
import pythoncom
import threading
import pickle
from datetime import datetime, timedelta, time
import telegram
import math
import numpy as np

# 서버접속 및 로그인 요청 이후 수신결과 데이터를 다르는 구간
class XASession:
    login_ok = False

    def OnLogin(self, szCode, szMsg):
        print("%s %s" % (szCode, szMsg), flush=True)

        if szCode == "0000":
            XASession.login_ok = True
            self.login_ok = True
        else:
            XASession.login_ok = False

class XAQuery:

    # 업종별종목
    t1516_event = None
    t1516_ok = False

    def OnReceiveData(self, szCode):
        if szCode == "t1516"

            shcode = self.GetFieldData("t1516OutBlock", "shcode", 0)
            pricejisu = self.GetFieldData("t1516OutBlock", "pricejisu", 0)
            sign = self.GetFieldData("t1516OutBlock", "sign", 0)
            change = self.GetFieldData("t1516OutBlock", "change", 0)
            jdiff = self.GetFieldData("t1516OutBlock", "jdiff", 0)
            # 보유종목 걧수 확인
            cnt = self.GetBlockCount("t1516OutBlock1")
            for i in range(cnt):
                hname = self.GetFieldData("t1516OutBlock1", "hname", i)
                price = self.GetFieldData("t1516OutBlock1", "price", i)
                sign = self.GetFieldData("t1516OutBlock1", "sign", i)
                change = self.GetFieldData("t1516OutBlock1", "change", i)
                diff = self.GetFieldData("t1516OutBlock1", "diff", i)
                volume = self.GetFieldData("t1516OutBlock1", "volume", i)
                open_pri = self.GetFieldData("t1516OutBlock1", "open", i)
                high = self.GetFieldData("t1516OutBlock1", "high", i)
                low = self.GetFieldData("t1516OutBlock1", "low", i)
                sojinrate = self.GetFieldData("t1516OutBlock1", "sojinrate", i)
                beta = self.GetFieldData("t1516OutBlock1", "beta", i)
                perx = self.GetFieldData("t1516OutBlock1", "perx", i)
                frgsvolume = self.GetFieldData("t1516OutBlock1", "frgsvolume", i)
                orgsvolume = self.GetFieldData("t1516OutBlock1", "orgsvolume", i)
                diff_vol = self.GetFieldData("t1516OutBlock1", "diff_vol", i)
                shcode = self.GetFieldData("t1516OutBlock1", "shcode", i)
                total = self.GetFieldData("t1516OutBlock1", "total", i)
                value = self.GetFieldData("t1516OutBlock1", "value", i)

                # print("종목번호: %s, 종목이름 %s" % (shcode, hname))
                if shcode in XAQuery.t1537_dict.keys():
                    XAQuery.t1537_dict[shcode].update({'업종명': '001'})
                    print(XAQuery.t1537_dict[shcode])

            # 다음 보유종목이 더 있을 경우 True
            if self.IsNext is True:
                Main.search_stock(upcode='001', shcode=shcode, IsNext=self.IsNext)
            else:
                XAQuery.t1516_ok = True

class Main: