# src/collectors/req_xaquery.py (데이터베이스 연동 버전)
import os
import time
import pythoncom
import win32com.client
import logging
from dotenv import load_dotenv
from typing import Optional, Dict, Any
from src.collectors.xaquery import XAQuery
from src.api.xasession import XASession
from src.core.integrated_database import IntegratedDatabaseManager

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ReqXAQuery:
    """LS증권 XingAPI 요청 처리 클래스 (데이터베이스 연동)"""

    def __init__(self):
        """초기화 및 로그인"""
        try:
            self._load_config()
            self._login()

            # 데이터베이스 매니저 초기화
            self.db_manager = IntegratedDatabaseManager()

            # XAQuery에 부모 인스턴스 설정 (순환참조 방지)
            XAQuery.set_parent(self)

            logger.info("✅ ReqXAQuery 초기화 완료 (DB 연동)")

        except Exception as e:
            logger.error(f"❌ 초기화 실패: {e}")
            raise

    def _load_config(self) -> None:
        """환경설정 로드"""
        try:
            load_dotenv()

            self.user_id = os.getenv('LS_USER_ID')
            self.password = os.getenv('LS_PASSWORD')
            self.cert_password = os.getenv('LS_CERT_PASSWORD')
            self.account_type = os.getenv('ACCOUNT_TYPE', 'demo')

            # 계정 정보 검증
            if not all([self.user_id, self.password, self.cert_password]):
                raise ValueError("필수 환경변수가 설정되지 않았습니다")

            # 서버 주소 설정
            if self.account_type == 'real':
                self.server_address = 'api.ls-sec.co.kr'
                logger.warning("🚨 실계좌 모드로 설정됨")
            else:
                self.server_address = 'demo.ls-sec.co.kr'
                logger.info("🧪 모의투자 모드로 설정됨")

        except Exception as e:
            logger.error(f"설정 로드 실패: {e}")
            raise

    def _login(self) -> None:
        """로그인 처리"""
        try:
            print("🔐 로그인 시작...")

            # 세션 생성
            self.session = win32com.client.DispatchWithEvents("XA_Session.XASession", XASession)

            # 서버 연결
            connect_result = self.session.ConnectServer(self.server_address, 20001)
            if connect_result != 1:
                raise Exception(f"서버 연결 실패: {connect_result}")

            # 로그인
            login_result = self.session.Login(self.user_id, self.password, self.cert_password, 0, False)
            if login_result != 1:
                raise Exception(f"로그인 요청 실패: {login_result}")

            # 로그인 대기 (타임아웃 추가)
            timeout = 0
            max_timeout = 300  # 30초
            while not XASession.login_ok and timeout < max_timeout:
                pythoncom.PumpWaitingMessages()
                time.sleep(0.1)
                timeout += 1

            if not XASession.login_ok:
                raise Exception("로그인 타임아웃")

            print("✅ 로그인 성공!")

        except Exception as e:
            logger.error(f"로그인 실패: {e}")
            raise

    def run_stock_themes_with_db_save(self) -> Dict[str, Any]:
        """테마 관련 데이터 수집 및 데이터베이스 저장"""
        try:
            logger.info("📊 테마 데이터 수집 및 DB 저장 시작")

            # 1. 누락 데이터 확인
            logger.info("🔄 누락 데이터 확인 중...")
            missing_stats = self.db_manager.update_missing_data()

            # 2. 이벤트 초기화
            self._init_events()

            # 3. 테마 전체 조회
            if not self._execute_theme_query():
                logger.error("테마 전체 조회 실패")
                return {}

            # 4. 각 테마별 종목 조회
            self._process_theme_items()

            # 5. 업종별 종목 조회
            self._execute_sector_query()

            # 6. 데이터베이스 저장
            save_result = self._save_collected_data()

            # 7. 결과 요약
            summary = self._generate_summary(missing_stats, save_result)

            logger.info("✅ 테마 데이터 수집 및 DB 저장 완료")
            return summary

        except Exception as e:
            logger.error(f"테마 데이터 수집 실패: {e}")
            return {}



    def _save_collected_data(self) -> Dict[str, Any]:
        """수집된 데이터를 데이터베이스에 저장"""
        try:
            logger.info("💾 수집 데이터 DB 저장 시작")

            save_results = {}

            # 1. 테마 데이터 저장
            if XAQuery.t8425_dict or XAQuery.t1537_dict:
                theme_saved = self.db_manager.save_theme_data(
                    XAQuery.t8425_dict,
                    XAQuery.t1537_dict
                )
                save_results['theme_data'] = theme_saved
                logger.info(f"✅ 테마 데이터 저장: {'성공' if theme_saved else '실패'}")

            # 2. 일봉 데이터 저장 (실시간 데이터 기반)
            # if XAQuery.t1537_dict:
            #     daily_saved = self.db_manager.save_daily_data_from_realtime(
            #         XAQuery.t1537_dict
            #     )
            #     save_results['daily_data'] = daily_saved
            #     logger.info(f"✅ 일봉 데이터 저장: {'성공' if daily_saved else '실패'}")
            save_results['daily_data'] = True  # 건너뛴 것으로 표시

            # 3. 데이터베이스 현황 조회
            db_summary = self.db_manager.get_database_summary()
            save_results['db_summary'] = db_summary

            return save_results

        except Exception as e:
            logger.error(f"데이터 저장 실패: {e}")
            return {}

    def _generate_summary(self, missing_stats: Dict, save_result: Dict) -> Dict[str, Any]:
        """실행 결과 요약 생성"""
        try:
            return {
                'collection_stats': {
                    'total_themes': len(XAQuery.t8425_dict),
                    'total_stocks': len(XAQuery.t1537_dict),
                    'collection_time': time.time()
                },
                'missing_data_stats': missing_stats,
                'save_results': save_result,
                'database_summary': save_result.get('db_summary', {}),
                'execution_time': time.time()
            }

        except Exception as e:
            logger.error(f"요약 생성 실패: {e}")
            return {}

    # 기존 메서드들 유지 (변경 없음)
    def _init_events(self) -> None:
        """이벤트 객체 초기화"""
        try:
            # t8425 테마전체조회
            if not self._init_query_event('t8425'):
                raise Exception("t8425 이벤트 초기화 실패")

            # t1537 테마종목별시세조회
            if not self._init_query_event('t1537'):
                raise Exception("t1537 이벤트 초기화 실패")

            # t1516 업종별종목
            if not self._init_query_event('t1516'):
                raise Exception("t1516 이벤트 초기화 실패")

            logger.info("✅ 모든 이벤트 초기화 완료")

        except Exception as e:
            logger.error(f"이벤트 초기화 실패: {e}")
            raise

    def _init_query_event(self, tr_code: str) -> bool:
        """개별 쿼리 이벤트 초기화"""
        try:
            # 리소스 파일 경로
            res_path = f"C:/eBEST/xingAPI/Res/{tr_code}.res"

            # 파일 존재 확인
            if not os.path.exists(res_path):
                logger.error(f"리소스 파일 없음: {res_path}")
                return False

            # 이벤트 생성
            event_obj = win32com.client.DispatchWithEvents("XA_Dataset.XAQuery", XAQuery)
            event_obj.ResFileName = res_path

            # 클래스 변수에 저장
            setattr(XAQuery, f"{tr_code}_event", event_obj)

            logger.info(f"✅ {tr_code} 이벤트 초기화 완료")
            return True

        except Exception as e:
            logger.error(f"{tr_code} 이벤트 초기화 실패: {e}")
            return False

    def _execute_theme_query(self) -> bool:
        """테마전체조회 실행"""
        try:
            print("📊 t8425 테마전체조회 시작...")

            # 완료 플래그 초기화
            XAQuery.t8425_ok = False
            XAQuery.t8425_dict.clear()

            # 요청 실행
            XAQuery.t8425_event.SetFieldData("t8425InBlock", "dummy", 0, "")
            err = XAQuery.t8425_event.Request(False)

            if err < 0:
                logger.error(f"t8425 요청 실패: {err}")
                return False

            # 응답 대기
            return self._wait_for_response('t8425', 't8425_ok', max_timeout=100)

        except Exception as e:
            logger.error(f"테마전체조회 실행 실패: {e}")
            return False

    # def _process_theme_items(self) -> None:
    #     """테마별 종목 조회 처리 (1번째, 265번째만)"""
    #     try:
    #         if not XAQuery.t8425_dict:
    #             logger.warning("조회할 테마가 없습니다")
    #             return
    #
    #         theme_list = list(XAQuery.t8425_dict.keys())
    #         target_indices = [1, 265]  # 1번째, 265번째
    #
    #         logger.info(f"📈 총 {len(theme_list)}개 테마 중 1번째, 265번째만 조회")
    #
    #         for idx in target_indices:
    #             # 인덱스 범위 체크 (0부터 시작하므로 idx-1)
    #             if idx <= len(theme_list):
    #                 tmcode = theme_list[idx - 1]  # 1번째 = 인덱스 0
    #                 XAQuery.current_tmcode = tmcode
    #
    #                 try:
    #                     logger.info(f"[{idx}번째] 테마 {tmcode} 조회 중...")
    #                     self._execute_theme_item_query(tmcode)
    #                     time.sleep(3.1)  # API 호출 제한 준수
    #
    #                 except Exception as e:
    #                     logger.error(f"테마 {tmcode} 조회 실패: {e}")
    #                     continue
    #             else:
    #                 logger.warning(f"{idx}번째 테마가 없습니다 (총 {len(theme_list)}개)")
    #
    #     except Exception as e:
    #         logger.error(f"테마별 종목 조회 처리 실패: {e}")

    def _process_theme_items(self) -> None:
        """테마별 종목 조회 처리"""
        try:
            if not XAQuery.t8425_dict:
                logger.warning("조회할 테마가 없습니다")
                return

            logger.info(f"📈 {len(XAQuery.t8425_dict)}개 테마의 종목 조회 시작")

            for i, tmcode in enumerate(XAQuery.t8425_dict.keys(), 1):

                # if i > 3:
                #     logger.info("🛑 3개 테마 조회 완료, 중지")
                #     break

                XAQuery.current_tmcode = tmcode
                try:
                    logger.info(f"[{i}/{len(XAQuery.t8425_dict)}] 테마 {tmcode} 조회 중...")
                    self._execute_theme_item_query(tmcode)
                    time.sleep(3.1)  # API 호출 제한 준수

                except Exception as e:
                    logger.error(f"테마 {tmcode} 조회 실패: {e}")
                    continue

        except Exception as e:
            logger.error(f"테마별 종목 조회 처리 실패: {e}")

    # def _process_theme_items(self) -> None:
    #     """테스트용: 첫 번째 테마만 조회"""
    #     try:
    #         if not XAQuery.t8425_dict:
    #             logger.warning("조회할 테마가 없습니다")
    #             return
    #
    #         # 🧪 테스트 모드: 첫 번째 테마만 처리
    #         first_theme_code = list(XAQuery.t8425_dict.keys())[0]
    #         first_theme_name = XAQuery.t8425_dict[first_theme_code].get('테마이름', '알수없음')
    #
    #         logger.info(f"🧪 테스트 모드: {first_theme_name}({first_theme_code}) 테마만 조회")
    #
    #         self._execute_theme_item_query(first_theme_code)
    #         time.sleep(3.1)
    #
    #     except Exception as e:
    #         logger.error(f"테마별 종목 조회 처리 실패: {e}")

    def _execute_theme_item_query(self, tmcode: str) -> bool:
        """테마종목별시세조회 실행"""
        try:
            if not XAQuery.t1537_event:
                logger.error("t1537 이벤트가 초기화되지 않음")
                return False

            XAQuery.t1537_ok = False
            XAQuery.t1537_event.SetFieldData("t1537InBlock", "tmcode", 0, tmcode)
            err = XAQuery.t1537_event.Request(False)

            if err < 0:
                logger.error(f"t1537 요청 실패 (테마: {tmcode}): {err}")
                return False

            return self._wait_for_response('t1537', 't1537_ok', max_timeout=50)

        except Exception as e:
            logger.error(f"테마종목별시세조회 실행 실패 (테마: {tmcode}): {e}")
            return False

    def _execute_sector_query(self) -> None:
        """업종별종목 조회 실행"""
        try:
            logger.info("📊 업종별종목 조회 시작")
            self.search_stock(upcode='001', shcode="", IsNext=False)

        except Exception as e:
            logger.error(f"업종별종목 조회 실패: {e}")

    def search_stock(self, upcode: str = None, shcode: str = None, IsNext: bool = False) -> bool:
        """업종별종목 검색"""
        try:
            if not XAQuery.t1516_event:
                logger.error("t1516 이벤트가 초기화되지 않음")
                return False

            time.sleep(3.1)  # API 호출 제한 준수

            XAQuery.t1516_ok = False
            XAQuery.t1516_event.SetFieldData("t1516InBlock", "upcode", 0, upcode or "")
            XAQuery.t1516_event.SetFieldData("t1516InBlock", "gubun", 0, "")
            XAQuery.t1516_event.SetFieldData("t1516InBlock", "shcode", 0, shcode or "")

            err = XAQuery.t1516_event.Request(IsNext)
            if err < 0:
                logger.error(f"t1516 요청 실패: {err}")
                return False

            return self._wait_for_response('t1516', 't1516_ok', max_timeout=100)

        except Exception as e:
            logger.error(f"업종별종목 검색 실패: {e}")
            return False

    def _wait_for_response(self, tr_code: str, flag_name: str, max_timeout: int = 100) -> bool:
        """응답 대기 (타임아웃 증가)"""
        try:
            print(f"⏳ {tr_code} 응답 대기 중...")

            timeout = 0
            # 🔧 t1537은 더 긴 타임아웃 적용
            if tr_code == 't1537':
                max_timeout = 200  # 20초로 증가

            while not getattr(XAQuery, flag_name) and timeout < max_timeout:
                pythoncom.PumpWaitingMessages()
                time.sleep(0.1)
                timeout += 1

            if timeout >= max_timeout:
                print(f"❌ {tr_code} 응답 타임아웃 ({max_timeout / 10}초)")
                return False

            print(f"✅ {tr_code} 응답 완료")
            return True

        except Exception as e:
            print(f"❌ {tr_code} 응답 대기 실패: {e}")
            return False

    def get_theme_summary(self) -> Dict[str, Any]:
        """테마 수집 결과 요약"""
        return {
            'total_themes': len(XAQuery.t8425_dict),
            'total_items': len(XAQuery.t1537_dict),
            'themes': list(XAQuery.t8425_dict.keys())[:10],  # 처음 10개만
            'collection_time': time.time()
        }

    def cleanup(self) -> None:
        """리소스 정리"""
        try:
            if hasattr(self, 'session') and self.session:
                self.session.DisconnectServer()
                logger.info("✅ 서버 연결 해제 완료")

        except Exception as e:
            logger.error(f"리소스 정리 실패: {e}")

    def __del__(self):
        """소멸자 - 리소스 정리"""
        self.cleanup()


if __name__ == "__main__":
    print("🚀 LS API Trading System 시작 (DB 연동)")
    req_query = None

    try:
        req_query = ReqXAQuery()

        # 🚨 핵심! 데이터베이스 연동 실행
        result = req_query.run_stock_themes_with_db_save()

        # 결과 출력
        if result:
            print(f"\n📊 실행 결과:")
            print(f"   수집 테마: {result.get('collection_stats', {}).get('total_themes', 0)}개")
            print(f"   수집 종목: {result.get('collection_stats', {}).get('total_stocks', 0)}개")

            # 데이터베이스 현황
            db_summary = result.get('database_summary', {})
            if db_summary:
                themes_info = db_summary.get('themes', {})
                print(f"   DB 테마: {themes_info.get('total_themes', 0)}개")
                print(f"   DB 매핑: {themes_info.get('total_mappings', 0)}개")

        print("✅ 시스템 실행 완료 (DB 저장 포함)")

    except Exception as e:
        logger.error(f"❌ 시스템 실행 실패: {e}")
        import traceback

        traceback.print_exc()

    finally:
        if req_query:
            req_query.cleanup()