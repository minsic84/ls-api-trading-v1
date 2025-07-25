import os
import win32com.client
import pythoncom
import threading
import pickle
from pykrx import stock
from datetime import datetime, timedelta, time
import telegram
from realtime_signal import *
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
    #예수금
    Dsp = None
    # 전체 계좌중 투자비율
    buy_ratio = 0.5
    # 전체 계좌중 투자금
    invest_money = None
    # 투자 종목 최대 갯수
    buy_cnt_limit = 3
    # 투자중 종목 수
    buy_count = 0
    # 한 종목당 투자금액
    money = None
    ############딕셔너리 관리################
    # 보유종목딕셔너리
    t0424_dict = {}
    # 미체결딕셔너리
    t0425_dict = {}
    # 테마코드용딕셔너리
    t8425_dict = {}
    # 특이테마딕셔너리
    t1533_dict = {}
    # 테마종목별시세딕셔너리
    t1537_dict = {}
    # 테마 리스트
    theme_list = []
    ########################################
    # 계좌번호
    CSPAQ12200_event = None
    CSPAQ12200_ok = False
    # 잔고내역
    t0424_event = None
    t0424_ok = False
    # 체결미체결
    t0425_event = None
    t0425_ok = False
    # 실시간 데이터
    t1857_event = None
    t1857_ok = False
    # 테마전체조회
    t8425_event = None
    t8425_ok = False
    # 섹터별종목
    t1531_event = None
    t1531_ok = False
    # 특이테마
    t1533_event = None
    t1533_ok = False
    # 테마종목별시세조회
    t1537_event = None
    t1537_ok = False
    # 업종별종목
    t1516_event = None
    t1516_ok = False

    def OnReceiveData(self, szCode):
        if szCode == "CSPAQ12200":
            # print("수신완료 %s" % szCode, flush=True)

            RecCnt = self.GetFieldData("CSPAQ12200OutBlock1", "RecCnt", 0)  # 레코드갯수
            MgmtBrnNo = self.GetFieldData("CSPAQ12200OutBlock1", "MgmtBrnNo", 0)
            AcntNo = self.GetFieldData("CSPAQ12200OutBlock1", "AcntNo", 0)
            Pwd = self.GetFieldData("CSPAQ12200OutBlock1", "Pwd", 0)
            BalCreTp = self.GetFieldData("CSPAQ12200OutBlock1", "BalCreTp", 0)
            BrnNm = self.GetFieldData("CSPAQ12200OutBlock2", "BrnNm", 0) #지점명
            AcntNm = self.GetFieldData("CSPAQ12200OutBlock2", "AcntNm", 0) #계좌명
            Dps = self.GetFieldData("CSPAQ12200OutBlock2", "Dps", 0) #예수금
            MgnRat100pctOrdAbleAmt = self.GetFieldData("CSPAQ12200OutBlock2", "MgnRat100pctOrdAbleAmt", 0) #증거금율 100퍼센트가능금액


            print("계좌번호: %s, 100%%증거금: %s" % (AcntNo, MgnRat100pctOrdAbleAmt))
            print("계좌번호: %s, 예수금: %s" % (AcntNo, Dps))
            # self.send_msg_telegram("계좌번호: %s, 100%%증거금: %s" % (AcntNo, MgnRat100pctOrdAbleAmt))

            XAQuery.Dsp = round(int(Dps))

            XAQuery.invest_money = round(int(MgnRat100pctOrdAbleAmt) * XAQuery.buy_ratio)
            XAQuery.money = XAQuery.invest_money // XAQuery.buy_cnt_limit  # 한 종목당 투자할수 있는 금액
            # XAQuery.money = 300000
            print("전체 매수할 금액 %s" % XAQuery.invest_money)
            print("종목당 매수 금액 %s" % XAQuery.money)

            XAQuery.CSPAQ12200_ok = True
            # threading.Timer(10.1, Main.deposit, args=[Main.acount_num, Main.acount_pw]).start()


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

        # 테마종목별시세조회
        elif szCode == "t1537":
            upcnt = self.GetFieldData("t1537OutBlock", "upcnt", 0)  # 상승종목수
            tmcnt = self.GetFieldData("t1537OutBlock", "tmcnt", 0)  # 테마종목수
            uprate = self.GetFieldData("t1537OutBlock", "uprate", 0)  # 상승종목비율
            tmname = self.GetFieldData("t1537OutBlock", "tmname", 0)  # 테마명

            tmcnt = self.safe_convert(int, tmcnt, 0, 'tmcnt')

            cnt = self.GetBlockCount("t1537OutBlock1")
            for i in range(cnt):
                hname = self.GetFieldData("t1537OutBlock1", "hname", i)  # 종목명
                price = self.GetFieldData("t1537OutBlock1", "price", i)  # 현재가
                sign = self.GetFieldData("t1537OutBlock1", "sign", i)  # 전일대비구분
                change = self.GetFieldData("t1537OutBlock1", "change", i)  # 전일대비
                diff = self.GetFieldData("t1537OutBlock1", "diff", i)  # 등락율
                volume = self.GetFieldData("t1537OutBlock1", "volume", i)  # 누적거래량
                jniltime = self.GetFieldData("t1537OutBlock1", "jniltime", i)  # 전일동시간
                shcode = self.GetFieldData("t1537OutBlock1", "shcode", i)  # 종목코드
                yeprice = self.GetFieldData("t1537OutBlock1", "yeprice", i)  # 예상체결가
                open = self.GetFieldData("t1537OutBlock1", "open", i)  # 시가
                high = self.GetFieldData("t1537OutBlock1", "high", i)  # 고가
                low = self.GetFieldData("t1537OutBlock1", "low", i)  # 저가
                value = self.GetFieldData("t1537OutBlock1", "value", i)  # 누적거래대금(단위:백만)
                marketcap = self.GetFieldData("t1537OutBlock1", "marketcap", i)  # 시가총액(단위:백만)

                price = self.safe_convert(int, price, 0, 'price')
                change = self.safe_convert(int, change, 0, 'change')
                diff = self.safe_convert(float, diff, 0.0, 'diff')
                volume = self.safe_convert(int, volume, 0, 'volume')
                yeprice = self.safe_convert(int, yeprice, 0, 'yeprice')
                open = self.safe_convert(int, open, 0, 'open')
                high = self.safe_convert(int, high, 0, 'high')
                low = self.safe_convert(int, low, 0, 'low')
                value = self.safe_convert(int, value, 0, 'value')
                marketcap = self.safe_convert(int, marketcap, 0, 'marketcap')
                day_date = datetime.today().strftime("%Y%m%d")

                data = [price, volume, value, day_date, open, low, high]
                self.sql_check(shcode, data)

                # 만약 키가 없다면 모든 데이터를 저장합니다.
                if shcode not in XAQuery.t1537_dict.keys():
                    XAQuery.t1537_dict[shcode] = {
                        "종목명": hname,
                        "테마명": [tmname],  # 테마명을 리스트로 저장
                        "테마종목수": tmcnt,
                        "현재가": price,
                        "전일대비구분": sign,
                        "전일대비": change,
                        "등락율": diff,
                        "누적거래량": volume,
                        "전일동시간": jniltime,
                        "예상체결가": yeprice,
                        "시가": open,
                        "고가": high,
                        "저가": low,
                        "누적거래대금": value,
                        "시가총액": marketcap
                    }
                else:
                    # 이미 존재하는 경우 '테마명'을 추가합니다.
                    if "테마명" not in XAQuery.t1537_dict[shcode]:
                        XAQuery.t1537_dict[shcode]["테마명"] = []
                    XAQuery.t1537_dict[shcode]["테마명"].append(tmname)
            print(f"테마명: {tmname}")

        # 특이테마
        if szCode == "t1533":
            bdate = self.GetFieldData("t1533OutBlock", "bdate", 0)  # 일자
            cnt = self.GetBlockCount("t1533OutBlock1")
            for i in range(cnt):
                tmname = self.GetFieldData("t1533OutBlock1", "tmname", i)  # 테마명
                totcnt = self.GetFieldData("t1533OutBlock1", "totcnt", i)  # 전체
                upcnt = self.GetFieldData("t1533OutBlock1", "upcnt", i)  # 상승
                dncnt = self.GetFieldData("t1533OutBlock1", "dncnt", i)  # 하락
                uprate = self.GetFieldData("t1533OutBlock1", "uprate", i)  # 상승비율
                diff_vol = self.GetFieldData("t1533OutBlock1", "diff_vol", i)  # 거래증가율
                avgdiff = self.GetFieldData("t1533OutBlock1", "avgdiff", i)  # 평균등락율
                chgdiff = self.GetFieldData("t1533OutBlock1", "chgdiff", i)  # 대비등락율
                tmcode = self.GetFieldData("t1533OutBlock1", "tmcode", i)  # 테마코드

                totcnt = self.safe_convert(int, totcnt, 0, 'totcnt')
                upcnt = self.safe_convert(int, upcnt, 0, 'upcnt')
                dncnt = self.safe_convert(int, dncnt, 0, 'dncnt')
                uprate = self.safe_convert(float, uprate, 0.0, 'uprate')
                diff_vol = self.safe_convert(float, diff_vol, 0.0, 'diff_vol')
                avgdiff = self.safe_convert(float, avgdiff, 0.0, 'avgdiff')
                chgdiff = self.safe_convert(float, chgdiff, 0.0, 'chgdiff')
                current_time = datetime.now().strftime("%H:%M:%S")

                # 조건에 따라 딕셔너리에 항목을 추가하거나 삭제하는 코드
                if uprate > 70 and avgdiff > 2:
                    item = {
                        'tmname': tmname,
                        'totcnt': totcnt,
                        'upcnt': upcnt,
                        'dncnt': dncnt,
                        'uprate': uprate,
                        'diff_vol': diff_vol,
                        'avgdiff': avgdiff,
                        'chgdiff': chgdiff,
                        'tmcode': tmcode,
                        'time': current_time
                    }
                    if tmcode not in XAQuery.t1533_dict:
                        XAQuery.t1533_dict[tmcode] = item  # 조건에 맞으면 딕셔너리에 저장
                else:
                    if tmcode in XAQuery.t1533_dict:
                        del XAQuery.t1533_dict[tmcode]  # 조건에 맞지 않으면 딕셔너리에서 삭제

            Main.meme_date_vol2()

            threading.Timer(1.1, Main.special_theme, args=["5", ""]).start()


        # 섹터별종목
        elif szCode == "t1531":
            cnt = self.GetBlockCount("t1531OutBlock")
            for i in range(cnt):
                tmname = self.GetFieldData("t1531OutBlock", "tmname", i)  # 테마명
                avgdiff = self.GetFieldData("t1531OutBlock", "avgdiff", i)  # 평균등락율
                tmcode = self.GetFieldData("t1531OutBlock", "tmcode", i)  # 테마코드

                print(f"테마명{tmname} 등락율{avgdiff} 테마코드{tmcode}")

        # 테마전체조회
        elif szCode == "t8425":
            # print("수신완료 %s" % szCode)

            cnt = self.GetBlockCount("t8425OutBlock")
            for i in range(cnt):
                tmname = self.GetFieldData("t8425OutBlock", "tmname", i)
                tmcode = self.GetFieldData("t8425OutBlock", "tmcode", i)

                if tmcode not in XAQuery.t8425_dict.keys():
                    XAQuery.t8425_dict[tmcode] = {'테마이름': tmname}

        # 매도로직(잔고내역)
        elif szCode == "t0424":
            # print("수신완료 %s" % szCode)

            sunamt = self.GetFieldData("t0424OutBlock", "sunamt", 0)  # 추정순자산
            dtsunik = self.GetFieldData("t0424OutBlock", "dtsunik", 0)  # 실현손익
            mamt = self.GetFieldData("t0424OutBlock", "mamt", 0)  # 매입금액
            sunamt1 = self.GetFieldData("t0424OutBlock", "sunamt1", 0)  # 추정D2예수금
            cts_expcode = self.GetFieldData("t0424OutBlock", "cts_expcode", 0)  # 종목번호
            tappamt = self.GetFieldData("t0424OutBlock", "tappamt", 0)  # 평가금액
            tdtsunik = self.GetFieldData("t0424OutBlock", "tdtsunik", 0)  # 평가손익

            # 보유종목 걧수 확인
            cnt = self.GetBlockCount("t0424OutBlock1")
            XAQuery.buy_count = int(cnt)
            for i in range(cnt):
                expcode = self.GetFieldData("t0424OutBlock1", "expcode", i)  # 종목번호
                jangb = self.GetFieldData("t0424OutBlock1", "jangb", i)  # 잔고구분
                janqty = self.GetFieldData("t0424OutBlock1", "janqty", i)  # 잔고수량
                mdposqt = self.GetFieldData("t0424OutBlock1", "mdposqt", i)  # 매도가능수량
                pamt = self.GetFieldData("t0424OutBlock1", "pamt", i)  # 평균단가
                mamt = self.GetFieldData("t0424OutBlock1", "mamt", i)  # 매입금액
                sinamt = self.GetFieldData("t0424OutBlock1", "sinamt", i)  # 대출금액
                lastdt = self.GetFieldData("t0424OutBlock1", "lastdt", i)  # 만기일자
                msat = self.GetFieldData("t0424OutBlock1", "msat", i)  # 당일매수금액
                mpms = self.GetFieldData("t0424OutBlock1", "mpms", i)  # 당일매수단가
                mdat = self.GetFieldData("t0424OutBlock1", "mdat", i)  # 당일매도금액
                mpmd = self.GetFieldData("t0424OutBlock1", "mpmd", i)  # 당일매도단가
                jsat = self.GetFieldData("t0424OutBlock1", "jsat", i)  # 전일매수금액
                jpms = self.GetFieldData("t0424OutBlock1", "jpms", i)  # 전일매수단가
                jdat = self.GetFieldData("t0424OutBlock1", "jdat", i)  # 전일매도금액
                jpmd = self.GetFieldData("t0424OutBlock1", "jpmd", i)  # 전일매도단가
                sysprocseq = self.GetFieldData("t0424OutBlock1", "sysprocseq", i)  # 처리순번
                loandt = self.GetFieldData("t0424OutBlock1", "loandt", i)  # 대출일자
                hname = self.GetFieldData("t0424OutBlock1", "hname", i)  # 종목명
                marketgb = self.GetFieldData("t0424OutBlock1", "marketgb", i)  # 시장구분
                jonggb = self.GetFieldData("t0424OutBlock1", "jonggb", i)  # 종목구분
                janrt = self.GetFieldData("t0424OutBlock1", "janrt", i)  # 보유비중
                price = self.GetFieldData("t0424OutBlock1", "price", i)  # 현재가
                appamt = self.GetFieldData("t0424OutBlock1", "appamt", i)  # 평가금액
                dtsunik = self.GetFieldData("t0424OutBlock1", "dtsunik", i)  # 평가손익
                sunikrt = self.GetFieldData("t0424OutBlock1", "sunikrt", i)  # 수익율
                fee = self.GetFieldData("t0424OutBlock1", "fee", i)  # 수수료
                tax = self.GetFieldData("t0424OutBlock1", "tax", i)  # 제세금
                sininter = self.GetFieldData("t0424OutBlock1", "sininter", i)  # 신용이자

                stock = expcode  # 종목번호
                name = hname  # 종목명

                # 변환 실패 시 기본 값으로 None을 사용
                investment_money = self.safe_convert(float, appamt, 0.0, 'appamt')  # 평가금액
                profit_n_loss = self.safe_convert(int, dtsunik, 0, 'dtsunik')  # 평가손익
                profit_margin = self.safe_convert(float, sunikrt, 0.0, 'sunikrt')  # 손익률
                price = self.safe_convert(int, price, 0.0, 'price')  # 현재가
                average = self.safe_convert(float, pamt, 0.0, 'pamt')  # 평균단가
                sellNo = self.safe_convert(int, mdposqt, 0, 'mdposqt')  # 매도가능수량

                if stock not in XAQuery.t0424_dict.keys():
                    XAQuery.t0424_dict[stock] = {}

                # 잔고수량이 없다면 딕셔너리에 삭제
                if sellNo == 0:
                    del XAQuery.t0424_dict[stock]
                else:
                    if XAQuery.buy_count > 0:
                        if sellNo > 0:
                            dates = Main.get_formatted_time()
                            self.handle_existing_shares(stock, name, price, dates, sellNo, average, investment_money,
                                                        profit_n_loss, profit_margin)

            # 다음 보유종목이 더 있을 경우 True
            if self.IsNext is True:
                threading.Timer(1.1, Main.deposit2,
                                args=[Main.acount_num, Main.acount_pw, cts_expcode, self.IsNext]).start()
            else:
                XAQuery.t0424_ok = True
                threading.Timer(1.1, Main.deposit2, args=[Main.acount_num, Main.acount_pw, cts_expcode, False]).start()


        # 체결미체결
        elif szCode == "t0425":
            # print(f"수신 -- {szCode}")
            tqty = self.GetFieldData("t0425OutBlock", "tqty", 0)  # 총주문수량
            tcheqty = self.GetFieldData("t0425OutBlock", "tcheqty", 0)  # 총체결수량
            tordrem = self.GetFieldData("t0425OutBlock", "tordrem", 0)  # 총미체결수량
            cmss = self.GetFieldData("t0425OutBlock", "cmss", 0)  # 추정수수료
            tamt = self.GetFieldData("t0425OutBlock", "tamt", 0)  # 총주문금액
            tmdamt = self.GetFieldData("t0425OutBlock", "tmdamt", 0)  # 총매도체결금액
            tmsamt = self.GetFieldData("t0425OutBlock", "tmsamt", 0)  # 총매수체결금액
            tax = self.GetFieldData("t0425OutBlock", "tax", 0)  # 추정제세금
            cts_ordno = self.GetFieldData("t0425OutBlock", "cts_ordno", 0)  # 주문번호

            # print(f"총주문수량{tqty} 주문번호{cts_ordno} 미체결잔량{tordrem}")
            cnt = self.GetBlockCount("t0425OutBlock1")
            for i in range(cnt):
                ordno = self.GetFieldData("t0425OutBlock1", "ordno", i)  # 주문번호
                expcode = self.GetFieldData("t0425OutBlock1", "expcode", i)  # 종목번호
                medosu = self.GetFieldData("t0425OutBlock1", "medosu", i)  # 구분
                qty = self.GetFieldData("t0425OutBlock1", "qty", i)  # 주문수량
                price = self.GetFieldData("t0425OutBlock1", "price", i)  # 주문가격
                cheqty = self.GetFieldData("t0425OutBlock1", "cheqty", i)  # 체결수량
                cheprice = self.GetFieldData("t0425OutBlock1", "cheprice", i)  # 체결가격
                ordrem = self.GetFieldData("t0425OutBlock1", "ordrem", i)  # 미체결잔량
                cfmqty = self.GetFieldData("t0425OutBlock1", "cfmqty", i)  # 확인수량
                status = self.GetFieldData("t0425OutBlock1", "status", i)  # 상태
                orgordno = self.GetFieldData("t0425OutBlock1", "orgordno", i)  # 원주문번호
                ordgb = self.GetFieldData("t0425OutBlock1", "ordgb", i)  # 유형
                ordtime = self.GetFieldData("t0425OutBlock1", "ordtime", i)  # 주문시간
                ordermtd = self.GetFieldData("t0425OutBlock1", "ordermtd", i)  # 주문매체
                sysprocseq = self.GetFieldData("t0425OutBlock1", "sysprocseq", i)  # 처리순번
                hogagb = self.GetFieldData("t0425OutBlock1", "hogagb", i)  # 호가유형
                price1 = self.GetFieldData("t0425OutBlock1", "price1", i)  # 현재가
                orggb = self.GetFieldData("t0425OutBlock1", "orggb", i)  # 주문구분
                singb = self.GetFieldData("t0425OutBlock1", "singb", i)  # 신용구분
                loandt = self.GetFieldData("t0425OutBlock1", "loandt", i)  # 대출일자

                # 변환 실패 시 기본 값으로 None을 사용
                ordno = self.safe_convert(int, ordno, 0, 'ordno')  # 주문번호
                ordrem = self.safe_convert(int, ordrem, 0, 'ordrem')  # 미체결잔량

                if expcode not in XAQuery.t0425_dict.keys():
                    XAQuery.t0425_dict[expcode] = {}

                # 미체결잔량이 없다면 딕셔너리에 삭제
                if ordrem == 0:
                    del XAQuery.t0425_dict[expcode]

                # print(f"종목번호{expcode} 주문번호{ordno} 주문가격{price} 구분{medosu} 미체결잔량{ordrem} 처리순번{sysprocseq}")

                # cansel = Main.check_value_in_data(expcode, '취소', 'N')
                # if not cansel:
                #     if medosu == '매수':
                #         if ordrem > 0:
                #             Main.cancel_order(OrgOrdNo=ordno, AcntNo=Main.acount_num, InptPwd=Main.acount_pw, IsuNo=expcode, OrdQty=ordrem)
                #         else:
                #             Main.replace_value_in_data(expcode, '취소', 'N')

            # 다음 보유종목이 더 있을 경우 True
            if self.IsNext is True:
                threading.Timer(1.1, Main.signing,
                                args=[Main.acount_num, Main.acount_pw, cts_ordno, self.IsNext]).start()
            else:
                XAQuery.t0425_ok = True
                threading.Timer(1.1, Main.signing, args=[Main.acount_num, Main.acount_pw, cts_ordno, False]).start()


    def sql_check(self, code, data):
        if code in Main.daychart_list:
            self.day_sql_value(code, data)
        else:
            self.day_sql_col(code)
            self.day_sql_value(code, data)

    def day_sql_value(self, code, data):
        table_name = 'y' + code
        today_date = data[3]

        try:
            conn = pymysql.connect(host='127.0.0.1', user='root', password='0000', charset='utf8', database='daychart')
            with conn.cursor() as curs:
                # 마지막 데이터의 날짜가 오늘 날짜와 같은지 확인
                sql_check_date = f"SELECT 일자 FROM `{table_name}` ORDER BY ID DESC LIMIT 1"
                curs.execute(sql_check_date)
                last_date = curs.fetchone()

                if last_date and last_date[0] == today_date:
                    # 오늘 날짜의 기존 데이터를 삭제
                    sql_delete = f"DELETE FROM `{table_name}` WHERE 일자 = %s"
                    curs.execute(sql_delete, (today_date,))

                # 새로운 데이터 삽입
                sql_value = f'''INSERT INTO `{table_name}` (현재가, 거래량, 거래대금, 일자, 시가, 저가, 고가) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)'''
                curs.execute(sql_value, data)
                conn.commit()
        finally:
            conn.close()

    def day_sql_col(self, code):
        table_name = 'y' + code
        sql_col = f'''
                      CREATE TABLE IF NOT EXISTS `{table_name}` (
                          ID INT AUTO_INCREMENT PRIMARY KEY,
                          현재가 INT NULL,
                          거래량 INT NULL,
                          거래대금 INT NULL,
                          일자 VARCHAR(30) NULL,
                          시가 INT NULL,
                          저가 INT NULL,
                          고가 INT NULL
                      )
                      '''

        try:
            conn = pymysql.connect(host='127.0.0.1', user='root', password='0000', charset='utf8', database='daychart')
            with conn.cursor() as curs:
                curs.execute(sql_col)
                conn.commit()
        finally:
            conn.close()

    def safe_convert(self, convert_func, value, default, value_name):
        try:
            return convert_func(value)
        except ValueError:
            print(f"Warning: Unable to convert {value_name} '{value}' to {convert_func.__name__}.")
            return default

    def moving_average(self, data, window):
        return sum(data[-window:]) / window

    # 표준 편차 계산 함수
    def standard_deviation(self, data, window):
        avg = self.moving_average(data, window)
        variance = sum((x - avg) ** 2 for x in data[-window:]) / window
        return math.sqrt(variance)

    # 볼린저 밴드 계산 함수
    def calculate_bollinger_bands(self, data, window=20, bb_std=3):
        if len(data) < window:
            raise ValueError("데이터가 충분하지 않습니다.")

        sma = self.moving_average(data, window)
        std_dev = self.standard_deviation(data, window)

        upper_band = sma + (std_dev * bb_std)
        lower_band = sma - (std_dev * bb_std)

        return sma, upper_band, lower_band

    def handle_existing_shares(self, stock, name, price, dates, sellNo, average, investment_money, profit_n_loss, profit_margin):
        # print(f"매도로직 {stock}")
        """
        investment_money    # 평가금액
        profit_n_loss       # 평가손익
        profit_margin       # 손익률
        price               # 현재가
        average             # 평균단가
        sellNo              # 매도가능수량
        """
        ###시간관리########
        now = datetime.now()

        # 현재 시간이 15시 18분인지 확인
        if now.hour == 15 and now.minute == 28:
            print("발생")
            ml10_window = Main.theme_stock_moveline[stock][10]
            print(ml10_window)
            print(price)
            ml10 = self.moving_average(ml10_window, 10)
            print(ml10)
            if price < ml10:
                print(f"15시18분조건 부합")
                self.sell_stock_two(stock, name, price, dates, sellNo, profit_margin, profit_n_loss, '매수여부')

        # if now.hour == 15 and now.minute == 30:
        #     Main.replace_value_in_data(stock, '매수여부', 'N')

        if not Main.check_value_in_data(stock, '손익', 'N'):

            # 손익 값을 가져옴
            loss_value = Main.get_value_from_data(stock, "손익", Main.signal_path)
            if loss_value is not None and loss_value!='본전매도':
                stop_loss_half = float(loss_value)

                # 수익률이 5% 이상일 때
                if profit_margin >= 5 and stop_loss_half == 5:
                    # 첫 번째 매도: 투자금의 20% 매도
                    sellNo = sellNo // 5  # 100% 중 20% 매도
                    self.sell_stock_two(stock, name, price, dates, sellNo, profit_margin, profit_n_loss, '손익')
                    # '손익' 값을 15로 변경하여 15% 이상 매도 조건 준비
                    Main.replace_value_in_data(stock, '손익', '15')


                # 수익률이 15% 이상일 때
                elif profit_margin >= 15 and stop_loss_half == 15:
                    # 두 번째 매도: 투자금의 30% 매도
                    sellNo = sellNo // 3  # 100% 중 30% 매도
                    self.sell_stock_two(stock, name, price, dates, sellNo, profit_margin, profit_n_loss, '손익')

                    # '손익' 값을 본전 매도로 업데이트
                    Main.replace_value_in_data(stock, '손익', '본전매도')
                    # '손절' 값을 0으로 설정
                    Main.replace_value_in_data(stock, '손절가격', average)

        # 수익 실현 후 나머지 주식 본전에서 매도
        if Main.get_value_from_data(stock, '손익', Main.signal_path) == '본전매도':
            # 현재 가격이 평균 단가에 도달했을 때 나머지 주식 전량 매도
            if profit_margin <= 0:
                self.sell_stock_two(stock, name, price, dates, sellNo, profit_margin, profit_n_loss, '본전매도')
                # 매도 후 '손익' 값을 N으로 리셋 (추후 재투자를 위한 초기화)
                Main.replace_value_in_data(stock, '손익', 'N')

        if not Main.check_value_in_data(stock, '손절가격', 'N'):
            loss_value = Main.get_value_from_data(stock, "손절가격", Main.signal_path)
            if loss_value is not None:
                stop_loss = int(loss_value)
                if price <= stop_loss:
                    self.sell_stock_one(stock, name, price, dates, sellNo, profit_margin, profit_n_loss, '손절가격')

    def sell_stock_one(self, stock, name, price, dates, sellNo, profit_margin, profit_n_loss, label):
        # 주식 전체 매도 처리
        '''
        호가유형코드              AcntNo  # 계좌번호
        OrdprcPtnCode           InptPwd  # 입력비밀번호
        00@지정가                IsuNo  # 종목번호
        03@시장가                OrdQty  # 주문수량
        05@조건부지정가           OrdPrc  # 주문가
        06@최유리지정가           BnsTpCode  # 매매구분(1:매도 2:매수)
        07@최우선지정가           OrdprcPtnCode  # 호가유형코드
        61@장개시전시간외종가      MgntrnCode  # 신용거래코드
        81@시간외종가             LoanDt  # 대출일
        82@시간외단일가           OrdCndiTpCode  # 주문조건구분(0:없음 1:IOC 2:FOK)
        '''
        selling_Quantity = sellNo
        Main.replace_value_in_data(stock, label, 'N')
        msg = f"{name}({stock})=={dates} 가격:{price} 매도:{label} 평가손익:{profit_n_loss} 손익률:{profit_margin} 주식수{selling_Quantity}/{sellNo}주"
        Main.send_msg_telegram(msg)
        Main.spot_normal_order(AcntNo=Main.acount_num, InptPwd=Main.acount_pw, IsuNo=stock,
                               OrdQty=selling_Quantity, OrdPrc="", BnsTpCode="1", OrdprcPtnCode="03",
                               MgntrnCode="000",
                               LoanDt="", OrdCndiTpCode="0")

    def sell_stock_two(self, stock, name, price, dates, sellNo, profit_margin, profit_n_loss, label):
        # 주식 전체 매도 처리
        '''
        호가유형코드              AcntNo  # 계좌번호
        OrdprcPtnCode           InptPwd  # 입력비밀번호
        00@지정가                IsuNo  # 종목번호
        03@시장가                OrdQty  # 주문수량
        05@조건부지정가           OrdPrc  # 주문가
        06@최유리지정가           BnsTpCode  # 매매구분(1:매도 2:매수)
        07@최우선지정가           OrdprcPtnCode  # 호가유형코드
        61@장개시전시간외종가      MgntrnCode  # 신용거래코드
        81@시간외종가             LoanDt  # 대출일
        82@시간외단일가           OrdCndiTpCode  # 주문조건구분(0:없음 1:IOC 2:FOK)
        '''
        selling_Quantity = sellNo
        Main.replace_value_in_data(stock, label, 'Y')
        msg = f"{name}({stock})=={dates} 가격:{price} 매도:{label} 평가손익:{profit_n_loss} 손익률:{profit_margin} 주식수{selling_Quantity}/{sellNo}주"
        Main.send_msg_telegram(msg)
        Main.spot_normal_order(AcntNo=Main.acount_num, InptPwd=Main.acount_pw, IsuNo=stock,
                               OrdQty=selling_Quantity, OrdPrc=price, BnsTpCode="1", OrdprcPtnCode="00",
                               MgntrnCode="000",
                               LoanDt="", OrdCndiTpCode="0")

    def OnReceiveMessage(self, systemError, messageCode, message):
        if systemError != 0:
            print("systemError: %s, messageCode: %s, message: %s" % (systemError, messageCode, message))
            msg = "systemError: %s, messageCode: %s, message: %s" % (systemError, messageCode, message)
            Main.send_msg_telegram(msg)



class XAReal:

    #틱데이터
    #코스피
    S3__Event = None
    #코스닥
    K3__Event = None
    # 호가잔량
    # 코스피
    H1__Event = None
    # 코스닥
    HA__Event = None
    # 지수
    IJ__Event = None
    #장시작마감확인
    JIF_Event = None
    #메세지보냄여부
    message_sent = False
    #RSI리스트
    rsi_list = []

    def OnReceiveRealData(self, szTrCode):
        # 코스피매수로직
        if szTrCode == "S3_":
            chetime = self.GetFieldData("OutBlock", "chetime") #체결시간
            sign = self.GetFieldData("OutBlock", "sign") #전일대비구분(1:상한, 2:상승, 3:보합, 4:하한, 5:하락)
            change = self.GetFieldData("OutBlock", "change") #전일대비
            drate = self.GetFieldData("OutBlock", "drate") #등락율
            price = self.GetFieldData("OutBlock", "price") #현재가
            opentime = self.GetFieldData("OutBlock", "opentime") #시가시간
            open = self.GetFieldData("OutBlock", "open") #시가
            hightime = self.GetFieldData("OutBlock", "hightime") #고가시간
            high = self.GetFieldData("OutBlock", "high") #고가
            lowtime = self.GetFieldData("OutBlock", "lowtime") #저가시간
            low = self.GetFieldData("OutBlock", "low") #저가
            cgubun = self.GetFieldData("OutBlock", "cgubun") #체결구분(+:매수, -:매도)
            cvolume = self.GetFieldData("OutBlock", "cvolume") #체결량
            volume = self.GetFieldData("OutBlock", "volume") #누적체결량
            value = self.GetFieldData("OutBlock", "value") #누적거래대금
            mdvolume = self.GetFieldData("OutBlock", "mdvolume") #매도누적체결량
            mdchecnt = self.GetFieldData("OutBlock", "mdchecnt") #매도누적체결건수
            msvolume = self.GetFieldData("OutBlock", "msvolume") #매수누적체결량
            mschecnt = self.GetFieldData("OutBlock", "mschecnt") #매수누적체결건수
            cpower = self.GetFieldData("OutBlock", "cpower") #체결강도
            w_avrg = self.GetFieldData("OutBlock", "w_avrg") #가중평균가
            offerho = self.GetFieldData("OutBlock", "offerho") #매도호가
            bidho = self.GetFieldData("OutBlock", "bidho") #매수호가
            status = self.GetFieldData("OutBlock", "status") #장정보
            jnilvolume = self.GetFieldData("OutBlock", "jnilvolume") #전일동시간대거래량
            shcode = self.GetFieldData("OutBlock", "shcode") #단축코드

            price = self.safe_convert(int, price, 0, 'price')  # 현재가
            drate = self.safe_convert(float, drate, 0.0, 'drate')  # 등락율
            value = self.safe_convert(int, value, 0, 'value')  # 누적거래대금
            volume = self.safe_convert(int, volume, 0, 'volume')  # 누적체결량
            high = self.safe_convert(int, high, 0, 'high')  # 고가
            cpower = self.safe_convert(float, cpower, 0.0, 'cpower')  # 체결강도
            cvolume = self.safe_convert(int, cvolume, 0, 'cvolume')  # 체결량
            open = self.safe_convert(int, open, 0, 'open')  # 시가
            low = self.safe_convert(int, low, 0, 'low')  # 저가

            self.process_real_time_data(shcode, chetime, price, open, low, cvolume, volume, cpower)
            # print(f"전일대비{sign}타입{type(sign)}")
        # 코스닥매수로직
        elif szTrCode == "K3_":
            chetime = self.GetFieldData("OutBlock", "chetime")  # 체결시간
            sign = self.GetFieldData("OutBlock", "sign")  # 전일대비구분
            change = self.GetFieldData("OutBlock", "change")  # 전일대비
            drate = self.GetFieldData("OutBlock", "drate")  # 등락율
            price = self.GetFieldData("OutBlock", "price")  # 현재가
            opentime = self.GetFieldData("OutBlock", "opentime")  # 시가시간
            open = self.GetFieldData("OutBlock", "open")  # 시가
            hightime = self.GetFieldData("OutBlock", "hightime")  # 고가시간
            high = self.GetFieldData("OutBlock", "high")  # 고가
            lowtime = self.GetFieldData("OutBlock", "lowtime")  # 저가시간
            low = self.GetFieldData("OutBlock", "low")  # 저가
            cgubun = self.GetFieldData("OutBlock", "cgubun")  # 체결구분
            cvolume = self.GetFieldData("OutBlock", "cvolume")  # 체결량
            volume = self.GetFieldData("OutBlock", "volume")  # 누적체결량
            value = self.GetFieldData("OutBlock", "value")  # 누적거래대금
            mdvolume = self.GetFieldData("OutBlock", "mdvolume")  # 매도누적체결량
            mdchecnt = self.GetFieldData("OutBlock", "mdchecnt")  # 매도누적체결건수
            msvolume = self.GetFieldData("OutBlock", "msvolume")  # 매수누적체결량
            mschecnt = self.GetFieldData("OutBlock", "mschecnt")  # 매수누적체결건수
            cpower = self.GetFieldData("OutBlock", "cpower")  # 체결강도
            w_avrg = self.GetFieldData("OutBlock", "w_avrg")  # 가중평균가
            offerho = self.GetFieldData("OutBlock", "offerho")  # 매도호가
            bidho = self.GetFieldData("OutBlock", "bidho")  # 매수호가
            status = self.GetFieldData("OutBlock", "status")  # 장정보
            jnilvolume = self.GetFieldData("OutBlock", "jnilvolume")  # 전일동시간대거래량
            shcode = self.GetFieldData("OutBlock", "shcode")  # 단축코드

            price = self.safe_convert(int, price, 0, 'price')  # 현재가
            drate = self.safe_convert(float, drate, 0.0, 'drate')  # 등락율
            value = self.safe_convert(int, value, 0, 'value')  # 누적거래대금
            volume = self.safe_convert(int, volume, 0, 'volume')  # 누적체결량
            high = self.safe_convert(int, high, 0, 'high')  # 고가
            cpower = self.safe_convert(float, cpower, 0.0, 'cpower')  # 체결강도
            cvolume = self.safe_convert(int, cvolume, 0, 'cvolume')  # 체결량
            open = self.safe_convert(int, open, 0, 'open')  # 시가
            low = self.safe_convert(int, low, 0, 'low')  # 저가

            self.process_real_time_data(shcode, chetime, price, open, low, cvolume, volume, cpower)
            # print(f"전일대비{sign}타입{type(sign)}")

        elif szTrCode == "H1_":
            hotime = self.GetFieldData("OutBlock", "hotime") #호가시간
            offerho1 = self.GetFieldData("OutBlock", "offerho1") #매도호가1
            bidho1 = self.GetFieldData("OutBlock", "bidho1") #매수호가1
            offerrem1 = self.GetFieldData("OutBlock", "offerrem1") #매도호가잔량1
            bidrem1 = self.GetFieldData("OutBlock", "bidrem1") # 매수호가잔량1
            offerho2 = self.GetFieldData("OutBlock", "offerho2") #매도호가2
            bidho2 = self.GetFieldData("OutBlock", "bidho2") #매수호가2
            offerrem2 = self.GetFieldData("OutBlock", "offerrem2")#매도호가잔량2
            bidrem2 = self.GetFieldData("OutBlock", "bidrem2")# 매수호가잔량2
            offerho3 = self.GetFieldData("OutBlock", "offerho3")#매도호가3
            bidho3 = self.GetFieldData("OutBlock", "bidho3")#매수호가3
            offerrem3 = self.GetFieldData("OutBlock", "offerrem3")#매도호가잔량3
            bidrem3 = self.GetFieldData("OutBlock", "bidrem3")#매수호가잔량3
            offerho4 = self.GetFieldData("OutBlock", "offerho4")#매도호가4
            bidho4 = self.GetFieldData("OutBlock", "bidho4")#매수호가4
            offerrem4 = self.GetFieldData("OutBlock", "offerrem4")#매도호가잔량4
            bidrem4 = self.GetFieldData("OutBlock", "bidrem4")#매수호가잔량4
            offerho5 = self.GetFieldData("OutBlock", "offerho5")
            bidho5 = self.GetFieldData("OutBlock", "bidho5")
            offerrem5 = self.GetFieldData("OutBlock", "offerrem5")
            bidrem5 = self.GetFieldData("OutBlock", "bidrem5")
            offerho6 = self.GetFieldData("OutBlock", "offerho6")
            bidho6 = self.GetFieldData("OutBlock", "bidho6")
            offerrem6 = self.GetFieldData("OutBlock", "offerrem6")
            bidrem6 = self.GetFieldData("OutBlock", "bidrem6")
            offerho7 = self.GetFieldData("OutBlock", "offerho7")
            bidho7 = self.GetFieldData("OutBlock", "bidho7")
            offerrem7 = self.GetFieldData("OutBlock", "offerrem7")
            bidrem7 = self.GetFieldData("OutBlock", "bidrem7")
            offerho8 = self.GetFieldData("OutBlock", "offerho8")
            bidho8 = self.GetFieldData("OutBlock", "bidho8")
            offerrem8 = self.GetFieldData("OutBlock", "offerrem8")
            bidrem8 = self.GetFieldData("OutBlock", "bidrem8")
            offerho9 = self.GetFieldData("OutBlock", "offerho9")
            bidho9 = self.GetFieldData("OutBlock", "bidho9")
            offerrem9 = self.GetFieldData("OutBlock", "offerrem9")
            bidrem9 = self.GetFieldData("OutBlock", "bidrem9")
            offerho10 = self.GetFieldData("OutBlock", "offerho10")
            bidho10 = self.GetFieldData("OutBlock", "bidho10")
            offerrem10 = self.GetFieldData("OutBlock", "offerrem10")
            bidrem10 = self.GetFieldData("OutBlock", "bidrem10")
            totofferrem = self.GetFieldData("OutBlock", "totofferrem")
            totbidrem = self.GetFieldData("OutBlock", "totbidrem")
            donsigubun = self.GetFieldData("OutBlock", "donsigubun")
            shcode = self.GetFieldData("OutBlock", "shcode")
            alloc_gubun = self.GetFieldData("OutBlock", "alloc_gubun")
            volume = self.GetFieldData("OutBlock", "volume")

            bidho1 = self.safe_convert(int, bidho1, 0, 'bidho1')  # 1호가
            bidho3 = self.safe_convert(int, bidho3, 0, 'bidho3')  # 3호가
            bidho5 = self.safe_convert(int, bidho5, 0, 'bidho5')  # 5호가
            bidho10 = self.safe_convert(int, bidho10, 0, 'bidho5')  # 10호가

            # # print("코스피종목 %s" % shcode)
            gubun = Main.get_value_from_data(shcode, '매수', Main.signal_path)
            # print(f"종목명{shcode}구분 {gubun}")
            if gubun == "Y":
                self.hoga_trade(shcode, hotime, bidho1, bidho5, bidho10)

        elif szTrCode == "HA_":
            hotime = self.GetFieldData("OutBlock", "hotime")  # 호가시간
            offerho1 = self.GetFieldData("OutBlock", "offerho1")  # 매도호가1
            bidho1 = self.GetFieldData("OutBlock", "bidho1")  # 매수호가1
            offerrem1 = self.GetFieldData("OutBlock", "offerrem1")  # 매도호가잔량1
            bidrem1 = self.GetFieldData("OutBlock", "bidrem1")  # 매수호가잔량1
            offerho2 = self.GetFieldData("OutBlock", "offerho2")  # 매도호가2
            bidho2 = self.GetFieldData("OutBlock", "bidho2")  # 매수호가2
            offerrem2 = self.GetFieldData("OutBlock", "offerrem2")  # 매도호가잔량2
            bidrem2 = self.GetFieldData("OutBlock", "bidrem2")  # 매수호가잔량2
            offerho3 = self.GetFieldData("OutBlock", "offerho3")  # 매도호가3
            bidho3 = self.GetFieldData("OutBlock", "bidho3")  # 매수호가3
            offerrem3 = self.GetFieldData("OutBlock", "offerrem3")  # 매도호가잔량3
            bidrem3 = self.GetFieldData("OutBlock", "bidrem3")  # 매수호가잔량3
            offerho4 = self.GetFieldData("OutBlock", "offerho4")  # 매도호가4
            bidho4 = self.GetFieldData("OutBlock", "bidho4")  # 매수호가4
            offerrem4 = self.GetFieldData("OutBlock", "offerrem4")  # 매도호가잔량4
            bidrem4 = self.GetFieldData("OutBlock", "bidrem4")  # 매수호가잔량4
            offerho5 = self.GetFieldData("OutBlock", "offerho5")
            bidho5 = self.GetFieldData("OutBlock", "bidho5")
            offerrem5 = self.GetFieldData("OutBlock", "offerrem5")
            bidrem5 = self.GetFieldData("OutBlock", "bidrem5")
            offerho6 = self.GetFieldData("OutBlock", "offerho6")
            bidho6 = self.GetFieldData("OutBlock", "bidho6")
            offerrem6 = self.GetFieldData("OutBlock", "offerrem6")
            bidrem6 = self.GetFieldData("OutBlock", "bidrem6")
            offerho7 = self.GetFieldData("OutBlock", "offerho7")
            bidho7 = self.GetFieldData("OutBlock", "bidho7")
            offerrem7 = self.GetFieldData("OutBlock", "offerrem7")
            bidrem7 = self.GetFieldData("OutBlock", "bidrem7")
            offerho8 = self.GetFieldData("OutBlock", "offerho8")
            bidho8 = self.GetFieldData("OutBlock", "bidho8")
            offerrem8 = self.GetFieldData("OutBlock", "offerrem8")
            bidrem8 = self.GetFieldData("OutBlock", "bidrem8")
            offerho9 = self.GetFieldData("OutBlock", "offerho9")
            bidho9 = self.GetFieldData("OutBlock", "bidho9")
            offerrem9 = self.GetFieldData("OutBlock", "offerrem9")
            bidrem9 = self.GetFieldData("OutBlock", "bidrem9")
            offerho10 = self.GetFieldData("OutBlock", "offerho10")
            bidho10 = self.GetFieldData("OutBlock", "bidho10")
            offerrem10 = self.GetFieldData("OutBlock", "offerrem10")
            bidrem10 = self.GetFieldData("OutBlock", "bidrem10")
            totofferrem = self.GetFieldData("OutBlock", "totofferrem")
            totbidrem = self.GetFieldData("OutBlock", "totbidrem")
            donsigubun = self.GetFieldData("OutBlock", "donsigubun")
            shcode = self.GetFieldData("OutBlock", "shcode")
            alloc_gubun = self.GetFieldData("OutBlock", "alloc_gubun")
            volume = self.GetFieldData("OutBlock", "volume")

            bidho1 = self.safe_convert(int, bidho1, 0, 'bidho1')  # 1호가
            bidho3 = self.safe_convert(int, bidho3, 0, 'bidho3')  # 3호가
            bidho5 = self.safe_convert(int, bidho5, 0, 'bidho5')  # 5호가
            bidho10 = self.safe_convert(int, bidho10, 0, 'bidho5')  # 10호가

            # # print("코스피종목 %s" % shcode)
            gubun = Main.get_value_from_data(shcode, '매수', Main.signal_path)
            # print(f"종목명{shcode}구분 {gubun}")
            if gubun == "Y":
                self.hoga_trade(shcode, hotime, bidho1, bidho5, bidho10)

        elif szTrCode == "IJ_":
            time = self.GetFieldData("OutBlock", "time") #시간
            jisu = self.GetFieldData("OutBlock", "jisu") #지수
            sign = self.GetFieldData("OutBlock", "sign") #전일대비구분
            change = self.GetFieldData("OutBlock", "change") #전일비
            drate = self.GetFieldData("OutBlock", "drate") #등락율
            cvolume = self.GetFieldData("OutBlock", "cvolume") #체결량
            volume = self.GetFieldData("OutBlock", "volume") #거래량
            value = self.GetFieldData("OutBlock", "value") #거래대금
            upjo = self.GetFieldData("OutBlock", "upjo") #상한종목수
            highjo = self.GetFieldData("OutBlock", "highjo") #상승종목수
            unchgjo = self.GetFieldData("OutBlock", "unchgjo") #보합종목수
            lowjo = self.GetFieldData("OutBlock", "lowjo") #하락종목수
            downjo = self.GetFieldData("OutBlock", "downjo") #하한종목수
            upjrate = self.GetFieldData("OutBlock", "upjrate") #상승종목비율
            openjisu = self.GetFieldData("OutBlock", "openjisu") #시가지수
            opentime = self.GetFieldData("OutBlock", "opentime") #시가시잔
            highjisu = self.GetFieldData("OutBlock", "highjisu") #고가지수
            hightime = self.GetFieldData("OutBlock", "hightime") #고가시간
            lowjisu = self.GetFieldData("OutBlock", "lowjisu") #저가지수
            lowtime = self.GetFieldData("OutBlock", "lowtime") #저가시간
            frgsvolume = self.GetFieldData("OutBlock", "frgsvolume") #외인순매수수량
            orgsvolume = self.GetFieldData("OutBlock", "orgsvolume") #기관순매수수량
            frgsvalue = self.GetFieldData("OutBlock", "frgsvalue") #외인순매수금액
            orgsvalue = self.GetFieldData("OutBlock", "orgsvalue") #기관순매수금액
            upcode = self.GetFieldData("OutBlock", "upcode") #업종코드

            # print(f"지수:{jisu} 전일대비구분:{sign} 상승종목비율:{upjrate}")

        elif szTrCode == "JIF":
            jangubun = self.GetFieldData("OutBlock", "jangubun")
            jstatus = self.GetFieldData("OutBlock", "jstatus")

            jangubun_signal = Main.jangubun_signals.get(jangubun, "Unknown jangubun")
            jstatus_signal = Main.jstatus_signals.get(jstatus, "Unknown jstatus")

            Main.send_msg_telegram(f"{jangubun}: {jangubun_signal} {jstatus}: {jstatus_signal}")

            print(f"{jangubun}: {jangubun_signal} {jstatus}: {jstatus_signal}")

    def hoga_trade(self, code, hotime, bidho1, bidho3, bidho5):
        hoga_list = [bidho1, bidho3, bidho5]
        name = Main.get_value_from_data(stock, '종목명')
        self.buy_condition(code, name, hotime, hoga_list)

    def process_real_time_data(self, stock, dates, price, open, low, cvolume, volume, cpower):


        """
        실시간 데이터를 처리하여 매수 및 매도 여부를 결정하는 함수

        매개변수:
        stock (str): 주식 코드
        data (dict): 실시간 주식 데이터 (예: 체결시간, 현재가 등)
        153040
        """
        Main.meme_date_vol3()

        # 주식 코드에 해당하는 이동평균선 데이터 가져오기
        data_dict = Main.theme_stock_moveline[stock]

        # 새로운 데이터를 기반으로 이동평균값 업데이트
        updated_moving_averages = self.update_moving_average(data_dict, price)
        ml_check = data_dict[5][-1]
        print(data_dict[10])

        #이평선
        ml5 = updated_moving_averages[5]
        ml10 = updated_moving_averages[10]
        print(ml10)
        ml20 = updated_moving_averages[20]
        ml60 = updated_moving_averages[60]
        ml120 = updated_moving_averages[120]
        ml200 = updated_moving_averages[200]


        if ml_check > 0 and  price > ml10 and ml5 > ml10 and price > ml20 and price > ml60 and price > ml120 and price > ml200:
            if stock not in XAReal.rsi_list:
                XAReal.rsi_list.append(stock)
                Main.meme_date_vol4(stock, price, dates)
        if Main.check_value_in_data(stock, '매수여부', 'N'):
            if stock in XAReal.rsi_list and price < ml5 and price > ml10:
                loss_check = self.can_invest(XAQuery.Dsp, price, ml10)
                if loss_check:
                    Main.replace_value_in_data(stock, '매수', 'Y')

        # # 이동평균선을 업데이트하여 최신 데이터 반영
        Main.theme_stock_moveline[stock] = self.update_values(data_dict, price)  # 이동평균 값 업데이트

    def can_invest(self, account_balance, purchase_price, ma10_price, max_loss_percentage=1):
        # 1. 전체 계좌의 1% 손실 가능 금액 계산
        max_loss = account_balance * (max_loss_percentage / 100)

        # 2. 매수 가격과 10일선 가격의 차이 계산
        price_difference = abs(purchase_price - ma10_price)

        # 3. 매수 가격과 손절 가격의 차이가 손실 가능 금액 이하인지 확인
        if price_difference <= max_loss:
            print(f"투자 가능: 가격 차이 {price_difference}원이 손실 허용 범위 {max_loss}원 이하입니다.")
            return True
        else:
            print(f"투자 불가능: 가격 차이 {price_difference}원이 손실 허용 범위 {max_loss}원을 초과합니다.")
            return False

    def buy_order(self, code, price, hotime, purchase_amount, quantity):
        #설정매수종목수
        if XAQuery.buy_count < XAQuery.buy_cnt_limit:
            #현재금액보다 매수금액이 높은지
            if purchase_amount > price:
                # 남아있는예수금이 계산된금액보다 많다면 (너무비싼주식은사지못한다)
                if purchase_amount < XAQuery.money:
                    # 최종매수진행
                    '''
                    호가유형코드              AcntNo  # 계좌번호
                    OrdprcPtnCode           InptPwd  # 입력비밀번호
                    00@지정가                IsuNo  # 종목번호              
                    03@시장가                OrdQty  # 주문수량             
                    05@조건부지정가           OrdPrc  # 주문가        
                    06@최유리지정가           BnsTpCode  # 매매구분(1:매도 2:매수)       
                    07@최우선지정가           OrdprcPtnCode  # 호가유형코드         
                    61@장개시전시간외종가      MgntrnCode  # 신용거래코드 
                    81@시간외종가             LoanDt  # 대출일          
                    82@시간외단일가           OrdCndiTpCode  # 주문조건구분(0:없음 1:IOC 2:FOK)
                    '''
                    # 호가위에서 체결 연구 필요!!!!!!!!
                    Main.spot_normal_order(AcntNo=Main.acount_num, InptPwd=Main.acount_pw, IsuNo=code,
                                           OrdQty=quantity, OrdPrc=price, BnsTpCode="2", OrdprcPtnCode="00",
                                           MgntrnCode="000", LoanDt="", OrdCndiTpCode="0")

                    msg = f"{Main.get_value_from_data(code, '종목명')}({code})=={hotime} 가격:{price} 금액:{XAQuery.money} 주식수 {quantity}주"
                    Main.send_msg_telegram(msg)
                    XAReal.message_sent = False  # 매수에 성공하면 메시지를 보낼 준비
                else:
                    if not XAReal.message_sent:
                        msg = f"({Main.get_value_from_data(code, '종목명')}{code})---가격:{price} 금액:{purchase_amount} 남아있는예수금이 계산된금액보다 많음"
                        Main.send_msg_telegram(msg)
                        XAReal.message_sent = True  # 실패 메시지를 보냈으므로 상태를 True로 설정
                        Main.replace_value_in_data(code, '매수', 'N')
            else:
                if not XAReal.message_sent:
                    msg = f"({Main.get_value_from_data(code, '종목명')}{code})---가격:{price} 금액:{purchase_amount} 매수금액이 현재가보다 낮음"
                    Main.send_msg_telegram(msg)
                    XAReal.message_sent = True  # 실패 메시지를 보냈으므로 상태를 True로 설정
                    Main.replace_value_in_data(code, '매수', 'N')
        else:
            if not XAReal.message_sent:
                msg = f"({Main.get_value_from_data(code, '종목명')}{code})---가격:{price} 금액:{purchase_amount} 종목수초과 매수 실패"
                Main.send_msg_telegram(msg)
                XAReal.message_sent = True  # 실패 메시지를 보냈으므로 상태를 True로 설정
                Main.replace_value_in_data(code, '매수', 'N')

    def buy_condition(self, code, name, dates, price_list):

        len_pri = len(price_list)
        current_pri = price_list[0]
        # 매수 횟수(num)에 따른 비중 설정
        buying_ratios = {
            1: 0.20,  # 첫 번째 매수: 20%
            2: 0.30,  # 두 번째 매수: 30%
            3: 0.50,  # 세 번째 매수: 50%
        }
        num = int(Main.get_value_from_data(code, '매수횟수', Main.signal_path))

        # 매수 비중 계산
        if num in buying_ratios:
            ratio = buying_ratios[num]
            mesu_money = XAQuery.money * ratio

        pers = -1  # 초기화
        for i in range(len_pri+1):
            # ZeroDivisionError 방지: 0으로 나누지 않도록 조건을 확인
            if len_pri - i != 0:
                purchase_amount = round(mesu_money // (len_pri - i))

                # 조건이 충족되면 pers 값을 설정하고 루프를 종료
                if purchase_amount > current_pri:
                    pers = len_pri - i
                    break

                print(f"Purchase amount when i={i}: {purchase_amount}")
            else:
                print(f"Division by zero avoided when i={i}")
        data_dict = Main.theme_stock_moveline[code]
        updated_moving_averages = self.update_moving_average(data_dict, current_pri)
        if pers > 0:
            for price in price_list[:pers]:
                if price > updated_moving_averages[10]:
                    quantity = purchase_amount // price  # 매수 주식 수 계산
                    self.buy_order(code, price, dates, purchase_amount, quantity)
            Main.replace_value_in_data(code, '매수여부', 'Y')
            Main.replace_value_in_data(code, '매수', 'N')
            # '매수횟수'를 문자열에서 정수로 변환
            num += 1
            # 다시 문자열로 변환하여 저장
            Main.replace_value_in_data(code, '매수횟수', str(num))
        elif pers == 0:
            if current_pri > updated_moving_averages[10]:
                price = current_pri
                quantity = 1  # 매수 주식 수
                self.buy_order(code, price, dates, purchase_amount, quantity)
            Main.replace_value_in_data(code, '매수여부', 'Y')
            Main.replace_value_in_data(code, '매수', 'N')
            # '매수횟수'를 문자열에서 정수로 변환
            num += 1
            # 다시 문자열로 변환하여 저장
            Main.replace_value_in_data(code, '매수횟수', str(num))

    def safe_convert(self, convert_func, value, default, value_name):
        try:
            return convert_func(value)
        except ValueError:
            print(f"Warning: Unable to convert {value_name} '{value}' to {convert_func.__name__}.")
            return default

    def update_moving_average(self, data_dict, price):
        # 이동 평균 업데이트
        """
                기존의 데이터와 새로운 데이터를 이용하여 이동평균을 업데이트합니다.

                :param data_dict: 기존 데이터가 담긴 딕셔너리
                :param new_data: 새로운 실시간 데이터 값 (예: 4600)
                :return: 업데이트된 이동평균 딕셔너리
                """
        updated_data_dict = {}

        for period, values in data_dict.items():
            updated_values = values[:-1] + [price]
            if len(updated_values) > period:
                updated_values = updated_values[-period:]

            moving_avg = sum(updated_values) // period
            # print(f"이전 이평평균선:{moving_avg}")
            adjusted_moving_avg = round(moving_avg * 1)
            # print(f"현재 이평평균선:{adjusted_moving_avg}")
            updated_data_dict[period] = adjusted_moving_avg

        return updated_data_dict

    def update_values(self, data_dict, price):
        # 값 업데이트
        """
               기존의 데이터 리스트를 업데이트합니다.

               :param data_dict: 기존 데이터가 담긴 딕셔너리
               :param new_data: 새로운 실시간 데이터 값 (예: 4600)
               :return: 업데이트된 데이터 리스트를 포함한 딕셔너리
               """
        updated_data_dict = {}

        for period, values in data_dict.items():
            # 새로운 데이터를 추가하여 업데이트합니다.
            updated_values = values[:-1] + [price]
            updated_data_dict[period] = updated_values

        return updated_data_dict

class Main:
    # 실계좌
    acount_num = "20664131401"
    acount_pw = "4802"
    passwd = "cho246!$"
    address = "api.ls-sec.co.kr"

    # 모의계좌
    # acount_num = '55503349801'
    # acount_pw = "0000"
    # passwd = "cho246"
    # address = "demo.ls-sec.co.kr"
    ####### MYSQL리스트 ######
    daychart_list = []
    ########이평선 업데이트####
    theme_stock_moveline = {}

    ## 매매txt ##
    file_path = r'C:\Users\C\Desktop\pythondata\data_files\real_text\real\analyze\rei_BB_real\six_pro_n_3vol.txt'
    signal_path = r"C:\Users\C\Desktop\pythondata\data_files\real_text\real\analyze\rei_BB_real\signal.txt"


    ##텔레그램###
    telegram_id = "6471007105:AAGdfi44WQtoS7G8ZMHnjX0MzE9irZs7A5o"
    signal_id = "5820612325:AAFkceMhlRLfYxR13JBTdYrxGS8s0GEFAAU"

    ##분석용 딕셔너리
    dict_file_path = 'C:/Users/C/Desktop/pythondata/data_files/real_text/real/pickle/items_by_theme.pkl'
    dict_file_before_path = 'C:/Users/C/Desktop/pythondata/data_files/real_text/real/pickle/items_by_theme_before.pkl'
    # 파일 존재 여부 확인
    if os.path.isfile(dict_file_path):
        with open(dict_file_path, 'rb') as f:  # 'rb' 모드로 파일 열기
            try:
                items_by_theme = pickle.load(f)
            except (pickle.PickleError, EOFError) as e:
                print(f"Error loading pickle file: {e}")
    else:
        print(f"File '{dict_file_path}' does not exist.")

    #########실시간 장구분######
    jangubun_signals = {
        "1": "코스피",
        "2": "코스닥",
        "5": "선물/옵션",
        "7": "CME야간선물",
        "8": "EUREX야간옵션선물",
        "9": "미국주식",
        "A": "중국주식오전",
        "B": "중국주식오후",
        "C": "홍콩주식오전",
        "D": "홍콩주식오후",
    }
    jstatus_signals = {
        "11": "장전동시호가개시",
        "21": "장시작",
        "22": "장개시10초전",
        "23": "장개시1분전",
        "24": "장개시5분전",
        "25": "장개시10분전",
        "31": "장후동시호가개시",
        "41": "장마감",
        "42": "장마감10초전",
        "43": "장마감1분전",
        "44": "장마감5분전",
        "51": "시간외종가매매개시",
        "52": "시간외종가매매종료,시간외단일가매매개시",
        "53": "사용안함",
        "54": "시간외단일가매매종료",
        "61": "서킷브레이크1단계발동",
        "62": "서킷브레이크1단계해제,호가접수개시",
        "63": "서킷브레이크1단계,동시호가종료",
        "64": "사이드카 매도발동",
        "65": "사이드카 매도해제",
        "66": "사이드카 매수발동",
        "67": "사이드카 매수해제",
        "68": "서킷브레이크2단계발동",
    }

    ##########################

    def __init__(self):
        print("클래스 실행")

        ##########SQL데이터베이스###
        self.connection = pymysql.connect(host='127.0.0.1', user='root', password='0000', charset='utf8')
        self.cursor = self.connection.cursor()
        self.tickbase = 'tickchart'
        self.daybase = 'daychart'
        ##########################

        #########리스트관리###################
        # 코스피 티커
        self.kospi_list = []
        # 코스닥 티커
        self.kosdaq_list = []
        ####################################

        #######전일날짜관리#############################
        # 현재 날짜를 datetime 객체로 가져옴
        today = datetime.today()
        # 어제 날짜 계산
        yesterday = today - timedelta(days=1)
        # 어제 날짜를 "YYYYMMDD" 형식으로 변환
        yesterday_str = yesterday.strftime("%Y%m%d")
        self.analysis_date = yesterday_str
        # self.analysis_date = "20240810"
        print(self.analysis_date)
        ##################################

        #######이평선 조건########
        self.days = 240
        self.windows = [5, 10, 20, 60, 120, 200]  # 이동평균선 윈도우 크기
        ########################

        #######당일날짜관리#############################
        self.today = datetime.today().strftime("%Y%m%d")
        ##################################

        self.read_code()
        self.signal_read_code()
        self.sql_row()
        self.populate_lists()
        print(f"코스피{self.kospi_list}")
        print(f"코스피갯수{len(self.kospi_list)}")
        print(f"코스닥{self.kosdaq_list}")
        print(f"코스닥갯수{len(self.kosdaq_list)}")
        self.evest()

    ####초기설정코드 (고정식)#######################################
    def read_code(self):
        # 중복 코드 제거
        unique_codes = []
        with open(Main.file_path, 'r', encoding='utf-8') as file:
            for line in file:
                code = line.strip().split()[0]
                # "종목코드:" 제거하고 코드만 추출
                code = code.replace('종목코드:', '')
                if code not in unique_codes:
                    unique_codes.append(code)

        # 중복 코드가 제거된 목록을 사용하여 작업
        self.codes = unique_codes

        for stock in self.codes:
            self.update_data_for_date(stock, self.analysis_date)

    def signal_read_code(self):
        # 중복 코드 제거
        with open(Main.signal_path, 'r', encoding='utf-8') as file:
            for line in file:
                code = line.strip().split()[0]
                # "종목코드:" 제거하고 코드만 추출
                code = code.replace('종목코드:', '')
                if code not in XAReal.rsi_list:
                    XAReal.rsi_list.append(code)
        print(XAReal.rsi_list)

    def update_data_for_date(self, stock, date):
        # 주어진 날짜의 데이터를 가져옴(날짜에 해당 분석 데이터 가져오기)
        query = f"""
                SELECT 일자, 현재가, 저가, 거래량, 거래대금 
                FROM y{stock} 
                WHERE DATE_FORMAT(일자, '%Y%m%d') <= '{date}' 
                ORDER BY 일자 DESC 
                LIMIT {self.days};
            """
        data = self.fetch_initial_data('daychart', query, "dic")
        self.move_line(stock, data)

    def fetch_initial_data(self, base, query, format):
        # 데이터베이스에서 초기 데이터 가져오기
        # SQL데이터 가져오기
        self.connection.select_db(base)  # 데이터베이스 선택
        if format == 'dic':
            cursor_class = pymysql.cursors.DictCursor
        elif format == 'lis':
            cursor_class = pymysql.cursors.Cursor  # 기본 커서
        with self.connection.cursor(cursor_class) as cursor:
            cursor.execute(query)
            data = cursor.fetchall()
        return data

    def move_line(self, stock, data):
        # 각 윈도우 크기에 대한 이동평균선 값을 저장할 리스트 초기화
        moveline_lists = [[0] * j for j in range(3, 241)]

        # 주어진 데이터를 기반으로 이동평균선 값을 계산
        for i in range(2, 240):  # 3~240
            for idx in range(i):
                if idx < len(data):
                    moveline_lists[i - 2][i - idx - 1] = data[idx]['현재가']

        # 계산된 이동평균선 값을 theme_stock_moveline 딕셔너리에 저장
        Main.theme_stock_moveline[stock] = {
            count: moveline_lists[count - 3] for count in self.windows
        }
        print(Main.theme_stock_moveline[stock])

    def sql_row(self, days='daychart'):
        conn = pymysql.connect(host='127.0.0.1', user='root', password='0000', charset='utf8')
        try:
            curs = conn.cursor()
            conn.select_db('%s' % days)
            conn.commit()
            curs.execute('SHOW TABLES')
            sql_row = curs.fetchall()

            for row in sql_row:
                pattern_punctuation = re.compile(r'[^\w\s]')
                output_string = pattern_punctuation.sub('', str(row))
                Main.daychart_list.append(output_string[1:])
        finally:
            conn.close()

    def populate_lists(self):
        for key, value in self.items_by_theme.items():
            if value.get('업종명') == '001':
                self.kospi_list.append(key)
            else:
                self.kosdaq_list.append(key)

    @staticmethod
    def meme_date_vol4(code, price, dates):
        existing_codes = []
        if os.path.isfile(Main.signal_path):
            with open(Main.signal_path, 'r', encoding='utf-8') as file:
                lines = [line.strip().split() for line in file]
                for line in lines:
                    if line:  # 라인이 비어 있지 않은 경우에만
                        existing_codes.append(line[0].replace('종목코드:', ''))
        else:
            print("파일이 존재하지 않습니다.")

        with open(Main.signal_path, 'a', encoding='utf-8') as file:
            if code not in existing_codes:
                dates_str = f"{dates[:2]}시{dates[2:4]}분{dates[4:6]}초"
                msg = f"{code} {Main.get_value_from_data(code, '종목명')}--신호발생--{price}원--시간:{dates_str}"
                Main.send2_msg_telegram(msg)
                file.write(
                    f"종목코드:{code}\t종목명:{Main.get_value_from_data(code, '종목명')}\t"
                    f"테마명:{Main.get_value_from_data(code, '테마명')}\t"
                    f"손절가격:{Main.get_value_from_data(code, '손절가격')}\t"
                    f"손절이평:{'10'}\t손익:{'5'}\t매수:{'N'}\t매수여부:{'N'}\t"
                    f"매수횟수:{'1'}\n")
    @staticmethod
    def meme_date_vol3():
        existing_codes = []
        if os.path.isfile(Main.signal_path):
            with open(Main.signal_path, 'r', encoding='utf-8') as file:
                lines = [line.strip().split() for line in file]
                for line in lines:
                    if line:  # 라인이 비어 있지 않은 경우에만
                        existing_codes.append(line[0].replace('종목코드:', ''))
        else:
            print("파일이 존재하지 않습니다.")

        XAReal.rsi_list = [item for item in XAReal.rsi_list if item in existing_codes]

    @staticmethod
    def meme_date_vol2():

        path_file = r"C:\Users\C\Desktop\pythondata\data_files\real_text\real\analyze\rei_BB_real\theme_list.txt"
        with open(path_file, 'w', encoding='utf-8') as file:
            for key, value in XAQuery.t1533_dict.items():
                file.write(
                    f"테마코드:{key}\t"
                    f"테마명:{value['tmname']}\t"
                    f"시간:{value['time']}\t"
                    f"상승비율:{value['uprate']}\t"
                    f"거래증가율:{value['diff_vol']}\t"
                    f"평균등락률:{value['avgdiff']}\n")

    @staticmethod
    def meme_date_vol():
        with open(Main.signal_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        # 필터링된 데이터 덮어쓰기
        with open(Main.signal_path, 'w', encoding='utf-8') as file:
            for line in lines:
                # 종목코드 추출
                code = line.split('\t')[0].split(':')[1]
                # 종목코드가 keep_codes에 있는 경우만 파일에 작성
                if code in XAQuery.t0424_dict.keys():
                    file.write(line)

    @staticmethod
    def calculate_rsi(data, window=28):
        if len(data) < window:
            raise ValueError("데이터가 충분하지 않습니다. 최소 28개 이상의 데이터가 필요합니다.")

        # 가격 변화 계산 (변동폭)
        deltas = [data[i] - data[i - 1] for i in range(1, len(data))]

        # 상승 및 하락 계산
        gains = [delta if delta > 0 else 0 for delta in deltas]
        losses = [-delta if delta < 0 else 0 for delta in deltas]

        # 첫 번째 14일 동안의 평균 상승과 하락 계산
        avg_gain = sum(gains[:window]) / window
        avg_loss = sum(losses[:window]) / window

        # 초기 RSI 계산
        rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')

        # 두 번째 14일 동안의 RSI를 초기 RSI를 기반으로 계산
        for i in range(window, len(deltas)):
            avg_gain = (avg_gain * (window - 1) + gains[i]) / window
            avg_loss = (avg_loss * (window - 1) + losses[i]) / window

        # 두 번째 RSI 계산
        rs = avg_gain / avg_loss if avg_loss != 0 else float('inf')
        rsi = 100 - (100 / (1 + rs))

        return rsi

    #################################이베스트요청관리##########################################
    def evest(self):
        ##########로그인부분#####################
        session = win32com.client.DispatchWithEvents("XA_Session.XASession", XASession)
        session.ConnectServer(Main.address, 20001)
        session.Login("mandoo40", Main.passwd, "chominc246!$", 0, False)

        while XASession.login_ok is False:
            pythoncom.PumpWaitingMessages()

        #############현물정상주문###################
        XAQuery.CSPAT00600_Event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.CSPAT00600_Event.ResFileName = "C:/LS_SEC/xingAPI/Res/CSPAT00600.res"

        #############현물정정주문###################
        XAQuery.CSPAT00700_Event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.CSPAT00700_Event.ResFileName = "C:/LS_SEC/xingAPI/Res/CSPAT00700.res"

        #############현물취소주문###################
        XAQuery.CSPAT00800_Event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.CSPAT00800_Event.ResFileName = "C:/LS_SEC/xingAPI/Res/CSPAT00800.res"

        ############예수금조회###################
        XAQuery.CSPAQ12200_event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.CSPAQ12200_event.ResFileName = "C:/LS_SEC/xingAPI/Res/CSPAQ12200.res"
        self.deposit(acc_no=Main.acount_num)

        ############체결미체결####################
        XAQuery.t0425_event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.t0425_event.ResFileName = "C:/LS_SEC/xingAPI/Res/t0425.res"
        self.signing(acc_no=Main.acount_num, acc_no_pwd=Main.acount_pw, cts_ordno="", IsNext=False)
        ###########테마전체조회#################
        XAQuery.t8425_event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.t8425_event.ResFileName = "C:/LS_SEC/xingAPI/Res/t8425.res"
        self.view_all_themems(dummy=None)

        ###########특이테마###################
        XAQuery.t1533_event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.t1533_event.ResFileName = "C:/LS_SEC/xingAPI/Res/t1533.res"
        # self.special_theme(gubun='5', chgdate='')

        ########### 업종별 종목시세 ############
        XAQuery.t1516_event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.t1516_event.ResFileName = "C:/LS_SEC/xingAPI/Res/t1516.res"
        # self.search_stock(upcode='001', shcode="", IsNext=False)

        ##########테마종목별시세조회############
        XAQuery.t1537_event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.t1537_event.ResFileName = "C:/LS_SEC/xingAPI/Res/t1537.res"
        # 특정 시간에 run_stock_themes 함수를 실행하기 위해 쓰레드를 생성합니다.
        run_time_thread = threading.Thread(target=Main.run_at_specific_time, args=(17, 58, 0, Main.run_stock_themes))
        run_time_thread.start()

        ############잔고내역####################
        # 잔고내역
        XAQuery.t0424_event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.t0424_event.ResFileName = "C:/LS_SEC/xingAPI/Res/t0424.res"
        self.deposit2(acc_no=Main.acount_num, acc_no_pwd=Main.acount_pw, cts_expcode="", IsNext=False)

        ###########섹터별종목#################
        XAQuery.t1531_event = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
        XAQuery.t1531_event.ResFileName = "C:/LS_SEC/xingAPI/Res/t1531.res"
        # self.items_by_sector(tmname='반도체 장비', tmcode='0012')



        ####################요청정보###############################

        # 틱데이터 요청
        # 코스피
        XAReal.S3__Event = win32com.client.DispatchWithEvents("XA_Dataset.XAReal", XAReal)
        XAReal.S3__Event.ResFileName = "C:/LS_SEC/xingAPI/Res/S3_.res"
        # 코스닥
        XAReal.K3__Event = win32com.client.DispatchWithEvents("XA_Dataset.XAReal", XAReal)
        XAReal.K3__Event.ResFileName = "C:/LS_SEC/xingAPI/Res/K3_.res"

        # 호가잔량
        # 코스피
        XAReal.H1__Event = win32com.client.DispatchWithEvents("XA_Dataset.XAReal", XAReal)
        XAReal.H1__Event.ResFileName = "C:/LS_SEC/xingAPI/Res/H1_.res"
        # 코스닥
        XAReal.HA__Event = win32com.client.DispatchWithEvents("XA_Dataset.XAReal", XAReal)
        XAReal.HA__Event.ResFileName = "C:/LS_SEC/xingAPI/Res/HA_.res"
        # 지수 요청
        # XAReal.IJ__Event = win32com.client.DispatchWithEvents("XA_Dataset.XAReal", XAReal)
        # XAReal.IJ__Event.ResFileName = "C:/LS_SEC/xingAPI/Res/IJ_.res"
        # XAReal.IJ__Event.SetFieldData("InBlock", "upcode", "301")
        # XAReal.IJ__Event.AdviseRealData()

        # 장구분
        XAReal.JIF_Event = win32com.client.DispatchWithEvents("XA_Dataset.XAReal", XAReal)
        XAReal.JIF_Event.ResFileName = "C:/LS_SEC/xingAPI/Res/JIF.res"
        XAReal.JIF_Event.SetFieldData("InBlock", "jangubun", "0")
        XAReal.JIF_Event.AdviseRealData()

        ###################루프관리#######################
        while True:
            for code in self.codes:
                self.pi_daq(code)
            pythoncom.PumpWaitingMessages()
        ################################################

    #############쿼리함수관련##################
    # 테마종목별시세조회
    @staticmethod
    def price_inquiry_by_theme_item(tmcode=None):
        XAQuery.t1537_event.SetFieldData("t1537InBlock", "tmcode", 0, tmcode)
        err = XAQuery.t1537_event.Request(False)

        if err < 0:
            print("테마종목별시세조회 에러")
            Main.send_msg_telegram("테마종목별시세조회 에러")

    # 특이테마
    @staticmethod
    def special_theme(gubun=None, chgdate=None):
        '''
        (구분)
        1:상승율 상위
        2:하락율 상위
        3:거래증가율 상위
        4:거래증가율 하위
        5:상승종목비율 상위
        6:상승종목비율 하위
        7:기준대비 상승율 상위
        8:기준대비 하락율 상위
        '''
        XAQuery.t1533_event.SetFieldData("t1533InBlock", "gubun", 0, gubun)  # 구분
        XAQuery.t1533_event.SetFieldData("t1533InBlock", "chgdate", 0, chgdate)  # 대비일자
        err = XAQuery.t1533_event.Request(False)

        if err < 0:
            print("특이테마 조회 에러")
            Main.send_msg_telegram("특이테마 조회 에러")

    # 업종별종목
    @staticmethod
    def search_stock(upcode=None, shcode=None, IsNext=False):

        time.sleep(3.1)

        XAQuery.t1516_event.SetFieldData("t1516InBlock", "upcode", 0, upcode)  # 업종코드
        XAQuery.t1516_event.SetFieldData("t1516InBlock", "gubun", 0, '')  # 구분
        XAQuery.t1516_event.SetFieldData("t1516InBlock", "shcode", 0, shcode)  # 종목코드
        XAQuery.t1516_event.Request(IsNext)

        XAQuery.t1516_ok = False
        while XAQuery.t1516_ok is False:
            pythoncom.PumpWaitingMessages()

    # 섹터별종목
    @staticmethod
    def items_by_sector(tmname=None, tmcode=None):
        XAQuery.t1531_event.SetFieldData("t1531InBlock", "tmname", 0, tmname)
        XAQuery.t1531_event.SetFieldData("t1531InBlock", "tmcode", 0, tmcode)
        err = XAQuery.t1531_event.Request(False)

        if err < 0:
            print("섹터별종목조회 에러")
            Main.send_msg_telegram("섹터별종목조회 에러")

    # 테마전체조회
    @staticmethod
    def view_all_themems(dummy=None):
        XAQuery.t8425_event.SetFieldData("t8425InBlock", "dummy", 0, dummy)
        err = XAQuery.t8425_event.Request(False)

        if err < 0:
            print("테마조회요청 에러")
            Main.send_msg_telegram("테마조회요청 에러")

    # 잔고내역
    @staticmethod
    def deposit2(acc_no=None, acc_no_pwd=None, cts_expcode=None, IsNext=False):

        time.sleep(0.51)

        XAQuery.t0424_event.SetFieldData("t0424InBlock", "accno", 0, acc_no)
        XAQuery.t0424_event.SetFieldData("t0424InBlock", "passwd", 0, acc_no_pwd)
        XAQuery.t0424_event.SetFieldData("t0424InBlock", "prcgb", 0, "1")
        XAQuery.t0424_event.SetFieldData("t0424InBlock", "chegb", 0, "2")
        XAQuery.t0424_event.SetFieldData("t0424InBlock", "dangb", 0, "0")
        XAQuery.t0424_event.SetFieldData("t0424InBlock", "charge", 0, "1")
        XAQuery.t0424_event.SetFieldData("t0424InBlock", "cts_expcode", 0, cts_expcode)
        XAQuery.t0424_event.Request(IsNext)  # True이면 다음 조회가능

        XAQuery.t0424_ok = False
        while XAQuery.t0424_ok is False:
            pythoncom.PumpWaitingMessages()

    @staticmethod
    def signing(acc_no=None, acc_no_pwd=None, cts_ordno=None, IsNext=False):

        time.sleep(0.51)

        XAQuery.t0425_event.SetFieldData("t0425InBlock", "accno", 0, acc_no)  # 계좌번호
        XAQuery.t0425_event.SetFieldData("t0425InBlock", "passwd", 0, acc_no_pwd)  # 비밀번호
        XAQuery.t0425_event.SetFieldData("t0425InBlock", "expcode", 0, "")  # 종목번호
        XAQuery.t0425_event.SetFieldData("t0425InBlock", "chegb", 0, "2")  # 체결구분 (0:전체, 1:체결, 2:미체결)
        XAQuery.t0425_event.SetFieldData("t0425InBlock", "medosu", 0, "0")  # 매매구분 (0:전체, 1:매도, 2:매수)
        XAQuery.t0425_event.SetFieldData("t0425InBlock", "sortgb", 0, "1")  # 정렬순서 (1:주문번호 역순, 2:주문번호 순)
        XAQuery.t0425_event.SetFieldData("t0425InBlock", "cts_ordno", 0,
                                         cts_ordno)  # 주문번호 (처음 조회시 스페이스, 연속조회시 cts_ordno)
        XAQuery.t0425_event.Request(IsNext)  # True이면 다음 조회가능

        XAQuery.t0425_ok = False
        while XAQuery.t0425_ok is False:
            pythoncom.PumpWaitingMessages()

    # 현물정상주문
    @staticmethod
    def spot_normal_order(AcntNo=None, InptPwd=None, IsuNo=None, OrdQty=None, OrdPrc=None, BnsTpCode=None,
                          OrdprcPtnCode=None, MgntrnCode=None, LoanDt=None, OrdCndiTpCode=None):

        XAQuery.CSPAT00600_Event.SetFieldData("CSPAT00600InBlock1", "AcntNo", 0, AcntNo)  # 계좌번호
        XAQuery.CSPAT00600_Event.SetFieldData("CSPAT00600InBlock1", "InptPwd", 0, InptPwd)  # 입력비밀번호
        XAQuery.CSPAT00600_Event.SetFieldData("CSPAT00600InBlock1", "IsuNo", 0, IsuNo)  # 종목번호
        XAQuery.CSPAT00600_Event.SetFieldData("CSPAT00600InBlock1", "OrdQty", 0, OrdQty)  # 주문수량
        XAQuery.CSPAT00600_Event.SetFieldData("CSPAT00600InBlock1", "OrdPrc", 0, OrdPrc)  # 주문가
        XAQuery.CSPAT00600_Event.SetFieldData("CSPAT00600InBlock1", "BnsTpCode", 0, BnsTpCode)  # 매매구분
        XAQuery.CSPAT00600_Event.SetFieldData("CSPAT00600InBlock1", "OrdprcPtnCode", 0, OrdprcPtnCode)  # 호가유형코드
        XAQuery.CSPAT00600_Event.SetFieldData("CSPAT00600InBlock1", "MgntrnCode", 0, MgntrnCode)  # 신용거래코드
        XAQuery.CSPAT00600_Event.SetFieldData("CSPAT00600InBlock1", "LoanDt", 0, LoanDt)  # 대출일
        XAQuery.CSPAT00600_Event.SetFieldData("CSPAT00600InBlock1", "OrdCndiTpCode", 0, OrdCndiTpCode)  # 주문조건구분
        err = XAQuery.CSPAT00600_Event.Request(False)

        if err < 0:
            print("현물주문요청 에러")
            Main.send_msg_telegram("현물주문요청 에러")

    # 취소주문
    @staticmethod
    def cancel_order(OrgOrdNo=None, AcntNo=None, InptPwd=None, IsuNo=None, OrdQty=None):
        XAQuery.CSPAT00800_Event.SetFieldData("CSPAT00800InBlock1", "OrgOrdNo", 0, OrgOrdNo)  # 원주문번호
        XAQuery.CSPAT00800_Event.SetFieldData("CSPAT00800InBlock1", "AcntNo", 0, AcntNo)  # 계좌번호
        XAQuery.CSPAT00800_Event.SetFieldData("CSPAT00800InBlock1", "InptPwd", 0, InptPwd)  # 입력비밀번호
        XAQuery.CSPAT00800_Event.SetFieldData("CSPAT00800InBlock1", "IsuNo", 0, IsuNo)  # 종목번호
        XAQuery.CSPAT00800_Event.SetFieldData("CSPAT00800InBlock1", "OrdQty", 0, OrdQty)  # 주문수량
        err = XAQuery.CSPAT00800_Event.Request(False)

        if err < 0:
            print("현물취소요청 에러")
            Main.send_msg_telegram("현물취소요청 에러")

    # 정정주문
    @staticmethod
    def correction_order(OrgOrdNo=None, AcntNo=None, InptPwd=None, IsuNo=None, OrdQty=None, OrdprcPtnCode=None,
                         OrdCndiTpCode=None, OrdPrc=None):
        XAQuery.CSPAT00700_Event.SetFieldData("CSPAT00700InBlock1", "OrgOrdNo", 0, OrgOrdNo)  # 원주문번호
        XAQuery.CSPAT00700_Event.SetFieldData("CSPAT00700InBlock1", "AcntNo", 0, AcntNo)  # 계좌번호
        XAQuery.CSPAT00700_Event.SetFieldData("CSPAT00700InBlock1", "InptPwd", 0, InptPwd)  # 입력비밀번호
        XAQuery.CSPAT00700_Event.SetFieldData("CSPAT00700InBlock1", "IsuNo", 0, IsuNo)  # 종목번호
        XAQuery.CSPAT00700_Event.SetFieldData("CSPAT00700InBlock1", "OrdQty", 0, OrdQty)  # 주문수량
        XAQuery.CSPAT00700_Event.SetFieldData("CSPAT00700InBlock1", "OrdprcPtnCode", 0, OrdprcPtnCode)  # 호가유형코드
        XAQuery.CSPAT00700_Event.SetFieldData("CSPAT00700InBlock1", "OrdCndiTpCode", 0, OrdCndiTpCode)  # 주문조건구분
        XAQuery.CSPAT00700_Event.SetFieldData("CSPAT00700InBlock1", "OrdPrc", 0, OrdPrc)  # 주문가
        err = XAQuery.CSPAT00700_Event.Request(False)

        if err < 0:
            print("현물정정요청 에러")
            Main.send_msg_telegram("현물정정요청 에러")

    # 예수금조회함수
    def deposit(self, acc_no=None, acc_no_pwd=acount_pw):

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

    ########## 틱데이터 요청 함수#########
    # 코스피
    def tick_kospi(self, code):
        XAReal.S3__Event.SetFieldData("InBlock", "shcode", code)
        XAReal.S3__Event.AdviseRealData()
    # 코스닥
    def tick_kosdaq(self, code):
        XAReal.K3__Event.SetFieldData("InBlock", "shcode", code)
        XAReal.K3__Event.AdviseRealData()
    ##################

    ############호가잔량
    #코스피
    def hoga_kospi(self, code):
        XAReal.H1__Event.SetFieldData("InBlock", "shcode", code)
        XAReal.H1__Event.AdviseRealData()
    #코스닥
    def hoga_kosdaq(self, code):
        XAReal.HA__Event.SetFieldData("InBlock", "shcode", code)
        XAReal.HA__Event.AdviseRealData()
    ####################

    ############################이베스트관리##########################################

    #####코스피or코스닥유무함수#########
    def pi_daq(self, code):
        if code in self.kospi_list:
            self.tick_kospi(code)
        else:
            self.tick_kosdaq(code)

    def pi_daq(self, code):
        if code in self.kospi_list:
            self.tick_kospi(code)
            self.hoga_kospi(code)
        else:
            self.tick_kosdaq(code)
            self.hoga_kosdaq(code)

    ######################외부공용함수관리################################################
    @staticmethod
    def run_at_specific_time(target_hour, target_minute, target_second, func, *args):
        while True:
            now = datetime.now()
            target_time = now.replace(hour=target_hour, minute=target_minute, second=target_second, microsecond=0)

            if now > target_time:
                target_time += timedelta(days=1)

            time_to_wait = (target_time - now).total_seconds()
            if time_to_wait > 0:
                print(f"Waiting for {time_to_wait} seconds until {target_time}.")
                time.sleep(time_to_wait)

            # 시간 도달 시 함수 실행
            func(*args)
    @staticmethod
    def run_stock_themes():
        Main.replace_value_in_data(stock, '매수여부', 'N')
        for theme in XAQuery.t8425_dict.keys():
            Main.price_inquiry_by_theme_item(theme)
            time.sleep(3.1)
        Main.run_search_stock(upcode='001')
        Main.save_dict_to_file(Main.dict_file_path)
        Main.analyze()
        Main.meme_date_vol()


    @staticmethod
    def run_search_stock(upcode=None):
        Main.search_stock(upcode=upcode, shcode="", IsNext=False)

    @staticmethod
    def save_dict_to_file(file_path):
        dictionary = XAQuery.t1537_dict
        # Ensure the directory exists
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Save the dictionary to the file
        with open(file_path, 'wb') as f:
            pickle.dump(dictionary, f)

        print(f"Dictionary saved to {file_path}")

    @staticmethod
    def analyze():

        fifteen_pro = r"C:\Users\C\Desktop\pythondata\data_files\real_text\real\analyze\analyze_data\fifteen_pro.txt"
        five_hundred_billion = r"C:\Users\C\Desktop\pythondata\data_files\real_text\real\analyze\analyze_data\five_hundred_billion.txt"
        six_pro_n_3vol = r"C:\Users\C\Desktop\pythondata\data_files\real_text\real\analyze\analyze_data\six_pro_n_3vol.txt"
        tenpro_tree_hunderd = r"C:\Users\C\Desktop\pythondata\data_files\real_text\real\analyze\analyze_data\tenpro_tree_hunderd.txt"

        # 파일 존재 여부 확인
        if os.path.isfile(Main.dict_file_before_path):
            with open(Main.dict_file_before_path, 'rb') as f:  # 'rb' 모드로 파일 열기
                try:
                    items_by_theme_befor = pickle.load(f)
                except (pickle.PickleError, EOFError) as e:
                    print(f"Error loading pickle file: {e}")
        else:
            print(f"File '{dict_file_path}' does not exist.")

        # 오늘 날짜를 'YYYYMMDD' 형식으로 얻기
        today_date = datetime.now().strftime('%Y%m%d')
        # 파일 존재 여부 확인
        if os.path.isfile(Main.dict_file_path):
            with open(Main.dict_file_path, 'rb') as f:  # 'rb' 모드로 파일 열기
                try:
                    items_by_theme = pickle.load(f)

                    # 조건에 맞는 항목 필터링
                    filtered_fifteen_pro = {}
                    filtered_five_hundred_billion = {}
                    filtered_six_pro_n_3vol = {}
                    filtered_tenpro_tree_hunderd = {}

                    for key, value in items_by_theme.items():
                        if isinstance(value, dict):
                            volume = value.get('누적거래대금', 0)  # 누적거래대금
                            vol = value.get('누적거래량', 0)  # 누적거래량
                            rate = value.get('등락율', 0)  # 등락율

                            if volume >= 100000 and rate >= 17:
                                filtered_tenpro_tree_hunderd[key] = value

                            # items_by_theme_befor에 키가 있는 경우에만 실행되는 부분
                            if key in items_by_theme_befor:
                                befor_vol = items_by_theme_befor[key]["누적거래량"]
                                vol_rate = Main.calculate_percentage_change(befor_vol, vol)
                                print(key)

                                if rate is not None and vol_rate is not None and rate >= 6 and vol_rate >= 300:
                                    filtered_six_pro_n_3vol[key] = value

                    # 파일에 코드와 이름 및 오늘 날짜 쓰기
                    with open(tenpro_tree_hunderd, 'a', encoding='utf-8') as file:
                        for key, value in filtered_tenpro_tree_hunderd.items():
                            if '업종명' in value:
                                gubun = '코스피'
                            else:
                                gubun = '코스닥'
                            msg = (
                                f"코드:{key}\t종목명:{value['종목명']}\t테마:{value['테마명']}\t등락율:{value['등락율']}\t현재가:{value['현재가']}거래대금{value['누적거래대금']}"
                                )
                            Main.send_msg_telegram(msg)
                            file.write(
                                f"{key}\t{value['종목명']}\t{today_date}\t{value['현재가']}\t{value['저가']}\t{value['등락율']}\t{'10pro3hun'}\t{gubun}\t{value['누적거래대금']}\t{value['테마명']}\n")

                    # 파일에 코드와 이름 및 오늘 날짜 쓰기
                    with open(six_pro_n_3vol, 'a', encoding='utf-8') as file:
                        for key, value in filtered_six_pro_n_3vol.items():
                            if '업종명' in value:
                                gubun = '코스피'
                            else:
                                gubun = '코스닥'
                            file.write(
                                f"종목코드:{key}\t종목명:{value['종목명']}\t날짜:{today_date}\t종가:{value['현재가']}\t저가:{value['저가']}\t등락율:{value['등락율']}\t구분:{'6pro3x'}\t시장구분:{gubun}\t테마명:{value['테마명']}\n")

                    # 결과 출력
                    for key, value in filtered_six_pro_n_3vol.items():
                        print(f"코드: {key}, 데이터: {value}")

                except (pickle.PickleError, EOFError) as e:
                    print(f"Error loading pickle file: {e}")
        else:
            print(f"File '{Main.dict_file_path}' does not exist.")

        Main.save_dict_to_file(Main.dict_file_before_path)

    @staticmethod
    def send_msg_telegram(msg):
        massage_bot = Main.telegram_id
        bot_id = "5463712358"
        bot = telegram.Bot(massage_bot)
        content = bot.sendMessage(chat_id=bot_id, text=msg)
        return content

    def send2_msg_telegram(msg):
        massage_bot = Main.signal_id
        bot_id = "5463712358"
        bot = telegram.Bot(massage_bot)
        content = bot.sendMessage(chat_id=bot_id, text=msg)
        return content

    @staticmethod
    def replace_value_in_data(stock, key, value):
        # 파일에서 모든 행을 읽어옵니다.
        with open(Main.signal_path, 'r', encoding='utf-8') as file:
            data = file.readlines()

        # 키와 값을 구분하는 접두사를 생성합니다.
        target_prefix = f"{key}:"
        updated_data = []

        # 각 행을 순회하면서 변경이 필요한 값을 찾고 업데이트합니다.
        for record in data:
            # 특정 코드가 포함된 행을 찾습니다.
            if f"코드:{stock}\t" in record:
                # 행을 탭으로 분리하여 필드 리스트로 변환합니다.
                fields = record.strip().split('\t')
                # 각 필드를 순회하면서 대상 키를 찾고 값을 업데이트합니다.
                for i in range(len(fields)):
                    if fields[i].startswith(target_prefix):
                        fields[i] = f"{key}:{value}"
                # 업데이트된 필드를 다시 탭으로 결합하여 행을 재구성합니다.
                updated_record = '\t'.join(fields) + '\n'
                updated_data.append(updated_record)
            else:
                # 대상 코드가 아닌 행은 그대로 추가합니다.
                updated_data.append(record)

        # 업데이트된 데이터를 파일에 다시 씁니다.
        with open(Main.signal_path, 'w', encoding='utf-8') as file:
            file.writelines(updated_data)

    #특정열 문자 확인용
    @staticmethod
    def check_value_in_data(stock, key, value):
        # 파일에서 모든 행을 읽어옵니다.
        with open(Main.signal_path, 'r', encoding='utf-8') as file:
            data = file.readlines()

        target_string = f"{key}:{value}"

        # 각 행을 순회하면서 특정 코드가 포함된 행을 찾고 값을 확인합니다.
        for record in data:
            # 특정 코드가 포함된 행을 찾습니다.
            if f"코드:{stock}\t" in record:
                # 행을 탭으로 분리하여 필드 리스트로 변환합니다.
                fields = record.strip().split('\t')
                # 각 필드를 순회하면서 대상 키와 값을 확인합니다.
                for field in fields:
                    if field == target_string:
                        return True
        return False

    # 지정값확인
    @staticmethod
    def get_value_from_data(stock, key, path=None):
        # 파일 경로를 결정합니다. path가 None이면 Main.signal_path를 사용합니다.
        file_path = path if path is not None else Main.file_path
        # 파일에서 모든 행을 읽어옵니다.
        with open(file_path, 'r', encoding='utf-8') as file:
            data = file.readlines()

        # 키와 값을 구분하는 접두사를 생성합니다.
        target_prefix = f"{key}:"

        # 각 행을 순회하면서 값을 찾습니다.
        for record in data:
            # 특정 코드가 포함된 행을 찾습니다.
            if f"코드:{stock}\t" in record:
                # 행을 탭으로 분리하여 필드 리스트로 변환합니다.
                fields = record.strip().split('\t')
                # 각 필드를 순회하면서 대상 키를 찾고 값을 가져옵니다.
                for field in fields:
                    if field.startswith(target_prefix):
                        # "key:value" 형식에서 value 부분을 반환합니다.
                        return field[len(target_prefix):]

        # 값을 찾지 못하면 None을 반환합니다.
        return None

    # 가격등락률계산
    @staticmethod
    def calculate_percentage_change(old_value, new_value):
        try:
            percentage_change = ((new_value - old_value) / abs(old_value)) * 100
            return round(percentage_change, 2)
        except ZeroDivisionError:
            # 예외 처리: 이전 가격이 0이라면 등락률을 정의할 수 없음
            return None

    #종목명확인
    @staticmethod
    def name_search(target_row_value):
        file_path = Main.file_path
        name = None  # 값을 찾지 못했을 경우를 대비해 기본값 설정
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                lines = file.readlines()

                for line in lines:
                    # 특정 행 찾기
                    if line.startswith(target_row_value):
                        # 특정 열의 값을 확인
                        columns = line.split()
                        name = columns[1]
                        break  # 특정 행을 찾았으므로 루프 종료
        except FileNotFoundError:
            print(f"File not found: {file_path}")
        except Exception as e:
            print(f"An error occurred: {e}")

        return name  # name을 반환

    @staticmethod
    def get_formatted_time():
        # 현재 시간 얻기
        current_time = datetime.now()
        # 시분초 형식으로 변환
        formatted_time = current_time.strftime("%H%M%S")
        time_str = f"{formatted_time[:2]}시{formatted_time[2:4]}분{formatted_time[4:6]}초"
        return time_str

    @staticmethod
    def calculate_average(values):
        # 평균값 계산
        total = sum(values)  # 리스트의 모든 요소의 합
        count = len(values)  # 리스트의 요소 수
        if count == 0:
            return 0  # 빈 리스트인 경우 평균은 0
        else:
            return total / count  # 평균 계산

if __name__=="__main__":
    Main()