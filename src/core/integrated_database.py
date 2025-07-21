# src/core/integrated_database.py
"""
통합 데이터베이스 시스템
- 실시간 데이터 저장
- 테마/종목 정보 관리
- 일봉 데이터 업데이트
"""
import logging
from datetime import datetime, date
from typing import Dict, Any, List, Optional
from src.core.database import MySQLMultiSchemaService
from src.utils.trading_date_calculator import TradingDateCalculator

logger = logging.getLogger(__name__)


class IntegratedDatabaseManager:
    """실시간 데이터와 데이터베이스 통합 관리"""

    def __init__(self):
        self.db = MySQLMultiSchemaService()
        self.date_calc = TradingDateCalculator()
        self._ensure_tables_exist()

    def _ensure_tables_exist(self):
        """필요한 테이블들 생성"""
        try:
            # 기본 테이블 생성
            self.db.create_tables()

            # 테마 관련 테이블 생성
            self._create_theme_tables()

            logger.info("✅ 데이터베이스 테이블 초기화 완료")

        except Exception as e:
            logger.error(f"❌ 테이블 초기화 실패: {e}")
            raise

    def _create_theme_tables(self):
        """테마 관련 테이블 생성"""
        try:
            conn = self.db._get_connection('main')
            cursor = conn.cursor()

            # 테마 정보 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS themes (
                    tmcode VARCHAR(10) PRIMARY KEY COMMENT '테마코드',
                    tmname VARCHAR(100) NOT NULL COMMENT '테마명',
                    is_active BOOLEAN DEFAULT TRUE COMMENT '활성 여부',
                    first_detected DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '최초 발견일',
                    last_updated DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '최종 업데이트',

                    INDEX idx_active (is_active),
                    INDEX idx_name (tmname)
                ) ENGINE=InnoDB COMMENT='테마 정보'
            """)

            # 테마-종목 매핑 테이블
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS theme_stocks (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    tmcode VARCHAR(10) NOT NULL COMMENT '테마코드',
                    stock_code VARCHAR(10) NOT NULL COMMENT '종목코드',
                    stock_name VARCHAR(100) COMMENT '종목명',
                    current_price INT DEFAULT 0 COMMENT '현재가',
                    change_rate DECIMAL(6,2) DEFAULT 0.00 COMMENT '등락율',
                    volume BIGINT DEFAULT 0 COMMENT '거래량',
                    market_cap BIGINT DEFAULT 0 COMMENT '시가총액',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

                    UNIQUE KEY uk_theme_stock (tmcode, stock_code),
                    INDEX idx_theme (tmcode),
                    INDEX idx_stock (stock_code),
                    INDEX idx_updated (updated_at),

                    FOREIGN KEY (tmcode) REFERENCES themes(tmcode) ON DELETE CASCADE
                ) ENGINE=InnoDB COMMENT='테마-종목 매핑'
            """)

            conn.commit()
            conn.close()

        except Exception as e:
            logger.error(f"테마 테이블 생성 실패: {e}")
            raise

    def save_theme_data(self, t8425_dict: Dict[str, Dict], t1537_dict: Dict[str, Dict]) -> bool:
        """테마 데이터 저장"""
        try:
            logger.info("📊 테마 데이터 저장 시작")

            # 1. 테마 정보 저장
            theme_count = self._save_themes(t8425_dict)

            # 2. 테마-종목 매핑 저장
            mapping_count = self._save_theme_stock_mappings(t1537_dict)

            logger.info(f"✅ 테마 데이터 저장 완료 - 테마: {theme_count}개, 매핑: {mapping_count}개")
            return True

        except Exception as e:
            logger.error(f"❌ 테마 데이터 저장 실패: {e}")
            return False

    def _save_themes(self, t8425_dict: Dict[str, Dict]) -> int:
        """테마 정보 저장"""
        try:
            if not t8425_dict:
                return 0

            conn = self.db._get_connection('main')
            cursor = conn.cursor()

            saved_count = 0
            for tmcode, data in t8425_dict.items():
                try:
                    tmname = data.get('테마이름', '알 수 없음')

                    query = """
                        INSERT INTO themes (tmcode, tmname, is_active)
                        VALUES (%s, %s, TRUE)
                        ON DUPLICATE KEY UPDATE
                            tmname = VALUES(tmname),
                            is_active = TRUE,
                            last_updated = CURRENT_TIMESTAMP
                    """

                    cursor.execute(query, (tmcode, tmname))
                    saved_count += 1

                except Exception as e:
                    logger.error(f"테마 저장 실패 ({tmcode}): {e}")
                    continue

            conn.commit()
            conn.close()

            return saved_count

        except Exception as e:
            logger.error(f"테마 정보 저장 실패: {e}")
            return 0

    def _save_theme_stock_mappings(self, t1537_dict: Dict[str, Dict]) -> int:
        """테마-종목 매핑 저장"""
        try:
            if not t1537_dict:
                return 0

            conn = self.db._get_connection('main')
            cursor = conn.cursor()

            saved_count = 0
            for stock_code, data in t1537_dict.items():
                try:
                    # 테마명이 리스트로 저장된 경우 처리
                    theme_names = data.get('테마명', [])
                    if not isinstance(theme_names, list):
                        theme_names = [theme_names]

                    stock_name = data.get('종목명', '알 수 없음')
                    current_price = self._safe_int(data.get('현재가', 0))
                    change_rate = self._safe_float(data.get('등락율', 0.0))
                    volume = self._safe_int(data.get('누적거래량', 0))
                    market_cap = self._safe_int(data.get('시가총액', 0))

                    # 각 테마별로 매핑 저장
                    for theme_name in theme_names:
                        if not theme_name:
                            continue

                        # 테마코드 찾기
                        tmcode = self._find_theme_code(cursor, theme_name)
                        if not tmcode:
                            continue

                        query = """
                            INSERT INTO theme_stocks (
                                tmcode, stock_code, stock_name, current_price,
                                change_rate, volume, market_cap
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                stock_name = VALUES(stock_name),
                                current_price = VALUES(current_price),
                                change_rate = VALUES(change_rate),
                                volume = VALUES(volume),
                                market_cap = VALUES(market_cap),
                                updated_at = CURRENT_TIMESTAMP
                        """

                        cursor.execute(query, (
                            tmcode, stock_code, stock_name, current_price,
                            change_rate, volume, market_cap
                        ))
                        saved_count += 1

                except Exception as e:
                    logger.error(f"매핑 저장 실패 ({stock_code}): {e}")
                    continue

            conn.commit()
            conn.close()

            return saved_count

        except Exception as e:
            logger.error(f"테마-종목 매핑 저장 실패: {e}")
            return 0

    def _find_theme_code(self, cursor, theme_name: str) -> Optional[str]:
        """테마명으로 테마코드 찾기"""
        try:
            cursor.execute("SELECT tmcode FROM themes WHERE tmname = %s", (theme_name,))
            result = cursor.fetchone()
            return result[0] if result else None

        except Exception as e:
            logger.error(f"테마코드 찾기 실패 ({theme_name}): {e}")
            return None

    def save_daily_data_from_realtime(self, realtime_data: Dict[str, Dict]) -> bool:
        """실시간 데이터를 일봉 형태로 저장"""
        try:
            if not realtime_data:
                return True

            logger.info(f"📈 실시간 데이터 일봉 저장 시작: {len(realtime_data)}개 종목")

            today = self.date_calc.get_market_today()
            today_str = today.strftime('%Y%m%d')

            saved_count = 0
            for stock_code, data in realtime_data.items():
                try:
                    # 일봉 데이터 형태로 변환
                    daily_data = {
                        'date': today_str,
                        'close_price': self._safe_int(data.get('현재가', 0)),
                        'open_price': self._safe_int(data.get('시가', 0)),
                        'high_price': self._safe_int(data.get('고가', 0)),
                        'low_price': self._safe_int(data.get('저가', 0)),
                        'volume': self._safe_int(data.get('누적거래량', 0)),
                        'trading_value': self._safe_int(data.get('누적거래대금', 0)),
                        'change_rate': self._safe_int(data.get('등락율', 0)),
                        'data_source': 'realtime_t1537',
                        'created_at': datetime.now()
                    }

                    # 일봉 데이터 저장
                    if self.db.save_daily_price_data(stock_code, [daily_data]):
                        saved_count += 1

                except Exception as e:
                    logger.error(f"일봉 저장 실패 ({stock_code}): {e}")
                    continue

            logger.info(f"✅ 일봉 데이터 저장 완료: {saved_count}/{len(realtime_data)}개")
            return True

        except Exception as e:
            logger.error(f"❌ 일봉 데이터 저장 실패: {e}")
            return False

    def update_missing_data(self, stock_codes: List[str] = None) -> Dict[str, Any]:
        """누락된 데이터 업데이트"""
        try:
            logger.info("🔄 누락 데이터 업데이트 시작")

            if not stock_codes:
                # 활성 종목 조회
                active_stocks = self.db.get_active_stock_codes()
                stock_codes = [stock['code'] for stock in active_stocks]

            update_stats = {
                'total_stocks': len(stock_codes),
                'updated_stocks': 0,
                'missing_days_found': 0,
                'errors': 0
            }

            for stock_code in stock_codes:
                try:
                    # 최신 데이터 날짜 조회
                    last_date = self.db.get_latest_daily_date(stock_code)

                    if last_date:
                        # 누락된 거래일 계산
                        missing_count, missing_dates = self.date_calc.count_missing_trading_days(last_date)

                        if missing_count > 0:
                            logger.info(f"{stock_code}: {missing_count}일 누락 ({last_date} 이후)")
                            update_stats['missing_days_found'] += missing_count
                            # 여기서 실제 데이터 수집 로직 호출 가능

                        update_stats['updated_stocks'] += 1

                except Exception as e:
                    logger.error(f"종목 업데이트 실패 ({stock_code}): {e}")
                    update_stats['errors'] += 1
                    continue

            logger.info(f"✅ 누락 데이터 업데이트 완료: {update_stats}")
            return update_stats

        except Exception as e:
            logger.error(f"❌ 누락 데이터 업데이트 실패: {e}")
            return {}

    def get_database_summary(self) -> Dict[str, Any]:
        """데이터베이스 현황 요약"""
        try:
            # 기본 테이블 정보
            table_info = self.db.get_table_info()

            # 테마 정보
            conn = self.db._get_connection('main')
            cursor = conn.cursor(dictionary=True)

            cursor.execute("SELECT COUNT(*) as theme_count FROM themes WHERE is_active = TRUE")
            theme_count = cursor.fetchone()['theme_count']

            cursor.execute("SELECT COUNT(*) as mapping_count FROM theme_stocks")
            mapping_count = cursor.fetchone()['mapping_count']

            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT stock_code) as unique_stocks,
                    AVG(current_price) as avg_price,
                    MAX(updated_at) as last_update
                FROM theme_stocks
            """)
            stock_stats = cursor.fetchone()

            conn.close()

            return {
                'tables': table_info,
                'themes': {
                    'total_themes': theme_count,
                    'total_mappings': mapping_count,
                    'unique_stocks': stock_stats['unique_stocks'],
                    'avg_price': float(stock_stats['avg_price']) if stock_stats['avg_price'] else 0,
                    'last_update': stock_stats['last_update']
                },
                'market_date': self.date_calc.get_market_today(),
                'generated_at': datetime.now()
            }

        except Exception as e:
            logger.error(f"데이터베이스 요약 조회 실패: {e}")
            return {}

    @staticmethod
    def _safe_int(value, default=0) -> int:
        """안전한 정수 변환"""
        try:
            if value is None or value == '':
                return default
            return int(float(str(value).replace(',', '')))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def _safe_float(value, default=0.0) -> float:
        """안전한 실수 변환"""
        try:
            if value is None or value == '':
                return default
            return float(str(value).replace(',', ''))
        except (ValueError, TypeError):
            return default


# 편의 함수
def get_integrated_db_manager() -> IntegratedDatabaseManager:
    """통합 데이터베이스 매니저 인스턴스 반환"""
    return IntegratedDatabaseManager()