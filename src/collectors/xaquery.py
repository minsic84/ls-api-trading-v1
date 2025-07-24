import win32com.client
import pythoncom
import time
import logging
from datetime import datetime
from typing import Optional, Dict, Any

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class XAQuery:
    """LS증권 XingAPI 쿼리 처리 클래스"""

    current_tmcode = None

    # 이벤트 객체들
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

    # 참조할 부모 클래스 (순환참조 방지)
    _parent_instance = None

    @classmethod
    def set_parent(cls, parent):
        """부모 인스턴스 설정 (Main 클래스 대신)"""
        cls._parent_instance = parent

    def OnReceiveData(self, szCode: str) -> None:
        """데이터 수신 이벤트 핸들러"""
        try:
            logger.info(f"📡 데이터 수신: {szCode}")

            if szCode == "t8425":
                self._handle_t8425_data()
            elif szCode == "t1537":
                self._handle_t1537_data()
            elif szCode == "t1516":
                self._handle_t1516_data()
            else:
                logger.warning(f"처리되지 않은 TR 코드: {szCode}")

        except Exception as e:
            logger.error(f"OnReceiveData 오류 ({szCode}): {e}")

    def _handle_t8425_data(self) -> None:
        """t8425 테마전체조회 데이터 처리"""
        try:
            print("✅ t8425 테마전체조회 수신완료!")

            cnt = self.GetBlockCount("t8425OutBlock")
            print(f"📊 총 테마 개수: {cnt}")

            if cnt > 0:
                print("=" * 60)
                for i in range(cnt):
                    tmname = self.GetFieldData("t8425OutBlock", "tmname", i)
                    tmcode = self.GetFieldData("t8425OutBlock", "tmcode", i)

                    # 딕셔너리에 저장 (안전하게)
                    if tmcode and tmcode not in XAQuery.t8425_dict:
                        XAQuery.t8425_dict[tmcode] = {
                            '테마이름': tmname or '알 수 없음',
                            '수신시간': datetime.now()
                        }

                    print(f"{i + 1:3d}. {tmname} ({tmcode})")
                print("=" * 60)
            else:
                print("❌ 테마 데이터가 없습니다.")

            # 완료 플래그 설정
            XAQuery.t8425_ok = True
            print("🎉 t8425 처리 완료!")

        except Exception as e:
            logger.error(f"t8425 데이터 처리 오류: {e}")
            XAQuery.t8425_ok = True  # 오류라도 플래그 설정

    def _handle_t1537_data(self) -> None:
        """t1537 테마종목별시세 데이터 처리 (디버깅 강화)"""
        try:
            upcnt = self.GetFieldData("t1537OutBlock", "upcnt", 0)
            tmcnt = self.GetFieldData("t1537OutBlock", "tmcnt", 0)
            uprate = self.GetFieldData("t1537OutBlock", "uprate", 0)
            tmname = self.GetFieldData("t1537OutBlock", "tmname", 0)

            cnt = self.GetBlockCount("t1537OutBlock1")
            logger.info(f"🔍 t1537 데이터 구조 확인: {cnt}개 종목")

            if cnt > 0:
                for i in range(cnt):  # 처음 3개만 상세 출력
                    # 🔍 모든 필드 확인
                    hname = self.GetFieldData("t1537OutBlock1", "hname", i)
                    price = self.GetFieldData("t1537OutBlock1", "price", i)
                    sign = self.GetFieldData("t1537OutBlock1", "sign", i)
                    change = self.GetFieldData("t1537OutBlock1", "change", i)
                    diff = self.GetFieldData("t1537OutBlock1", "diff", i)
                    volume = self.GetFieldData("t1537OutBlock1", "volume", i)
                    shcode = self.GetFieldData("t1537OutBlock1", "shcode", i)
                    yeprice = self.GetFieldData("t1537OutBlock1", "yeprice", i)
                    open_val = self.GetFieldData("t1537OutBlock1", "open", i)
                    high = self.GetFieldData("t1537OutBlock1", "high", i)
                    low = self.GetFieldData("t1537OutBlock1", "low", i)
                    value = self.GetFieldData("t1537OutBlock1", "value", i)
                    marketcap = self.GetFieldData("t1537OutBlock1", "marketcap", i)

                    # 기존 저장 로직
                    if shcode:
                        XAQuery.t1537_dict[shcode] = {
                            'hname': hname,
                            'price': price,
                            'open': open_val,
                            'high': high,
                            'low': low,
                            'volume': volume,
                            'value': value,
                            'diff': diff,
                            'marketcap': marketcap,
                            'tmcode': XAQuery.current_tmcode,
                            '수신시간': datetime.now()
                        }

            XAQuery.t1537_ok = True
            print(f"🎉 t1537 처리 완료! 총 {len(XAQuery.t1537_dict)}개 종목")

        except Exception as e:
            logger.error(f"t1537 데이터 처리 오류: {e}")
            XAQuery.t1537_ok = True

    def _handle_t1516_data(self) -> None:
        """t1516 업종별종목 데이터 처리"""
        try:
            # 헤더 정보
            shcode = self.GetFieldData("t1516OutBlock", "shcode", 0)
            pricejisu = self.GetFieldData("t1516OutBlock", "pricejisu", 0)

            # 종목 데이터
            cnt = self.GetBlockCount("t1516OutBlock1")
            logger.info(f"t1516 종목 수: {cnt}")

            for i in range(cnt):
                hname = self.GetFieldData("t1516OutBlock1", "hname", i)
                price = self.GetFieldData("t1516OutBlock1", "price", i)
                shcode_item = self.GetFieldData("t1516OutBlock1", "shcode", i)
                volume = self.GetFieldData("t1516OutBlock1", "volume", i)

                # t1537_dict에 업종명 추가 (안전하게)
                if shcode_item and shcode_item in XAQuery.t1537_dict:
                    XAQuery.t1537_dict[shcode_item].update({'업종명': '001'})

            # 다음 데이터 확인
            if hasattr(self, 'IsNext') and self.IsNext:
                # 부모 클래스를 통해 안전하게 호출
                if XAQuery._parent_instance and hasattr(XAQuery._parent_instance, 'search_stock'):
                    XAQuery._parent_instance.search_stock(upcode='001', shcode=shcode, IsNext=self.IsNext)
            else:
                XAQuery.t1516_ok = True

        except Exception as e:
            logger.error(f"t1516 데이터 처리 오류: {e}")
            XAQuery.t1516_ok = True

    def GetFieldData(self, block_name: str, field_name: str, index: int = 0) -> Optional[str]:
        """필드 데이터 가져오기 (안전한 래퍼)"""
        try:
            # 실제 구현에서는 COM 객체 호출
            return "mock_data"  # 테스트용
        except Exception as e:
            logger.error(f"GetFieldData 오류 ({block_name}.{field_name}[{index}]): {e}")
            return None

    def GetBlockCount(self, block_name: str) -> int:
        """블록 개수 가져오기 (안전한 래퍼)"""
        try:
            # 실제 구현에서는 COM 객체 호출
            return 5  # 테스트용
        except Exception as e:
            logger.error(f"GetBlockCount 오류 ({block_name}): {e}")
            return 0

    @property
    def IsNext(self) -> bool:
        """다음 데이터 존재 여부"""
        try:
            # 실제 구현에서는 COM 객체 속성 확인
            return False  # 테스트용
        except Exception as e:
            logger.error(f"IsNext 확인 오류: {e}")
            return False