import win32com.client
import pythoncom
import time
from datetime import datetime


class XAQuery:
    CSPAQ12200_event = None
    CSPAQ12200_ok = False

    t1537_event = None
    t1537_ok = False
    t1537_dict = {}

    t8425_event = None
    t8425_ok = False
    t8425_dict = {}

    t1516_event = None
    t1516_ok = False

    def OnReceiveData(self, szCode):
        print(f"📡 데이터 수신: {szCode}")

        if szCode == "t8425":
            print("✅ t8425 테마전체조회 수신완료!")

            cnt = self.GetBlockCount("t8425OutBlock")
            print(f"📊 총 테마 개수: {cnt}")

            if cnt > 0:
                print("=" * 60)
                for i in range(cnt):
                    tmname = self.GetFieldData("t8425OutBlock", "tmname", i)
                    tmcode = self.GetFieldData("t8425OutBlock", "tmcode", i)

                    # 딕셔너리에 저장
                    if tmcode not in XAQuery.t8425_dict:
                        XAQuery.t8425_dict[tmcode] = {'테마이름': tmname}

                    print(f"{i + 1:3d}. {tmname} ({tmcode})")
                print("=" * 60)
            else:
                print("❌ 테마 데이터가 없습니다.")

            # 🚨 핵심! 완료 플래그 설정
            XAQuery.t8425_ok = True
            print("🎉 t8425 처리 완료!")

        elif szCode == "t1537":
            cnt = self.GetBlockCount("t1537OutBlock1")
            for i in range(cnt):
                hname = self.GetFieldData("t1537OutBlock1", "hname", i)
                price = self.GetFieldData("t1537OutBlock1", "price", i)
                shcode = self.GetFieldData("t1537OutBlock1", "shcode", i)
                XAQuery.t1537_dict[shcode] = {
                    'hname': hname,
                    'price': price
                }
            XAQuery.t1537_ok = True

        # 업종별종목(코스피,코스닥)
        elif szCode == "t1516":

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

    def GetFieldData(self, *args):
        return "mock_data"  # 실제 구현에서는 진짜 데이터 반환

    def GetBlockCount(self, *args):
        return 5  # 테스트용으로 5개 반환

    def IsNext(self):
        return False