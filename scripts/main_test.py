import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.collectors.req_xaquery import ReqXAQuery


class Main:
    def __init__(self):
        print("Main 클래스 초기화됨")


if __name__ == "__main__":
    print("🚀 메인 실행 시작 (데이터베이스 연동)")
    req_query = None

    try:
        # ReqXAQuery 인스턴스 생성 (로그인까지 완료)
        req_query = ReqXAQuery()

        # 🚨 핵심! 테마 데이터 수집 + 데이터베이스 저장
        print("📊 테마 데이터 수집 및 DB 저장 시작...")
        result = req_query.run_stock_themes_with_db_save()

        # 상세 결과 출력
        if result:
            collection_stats = result.get('collection_stats', {})
            missing_stats = result.get('missing_data_stats', {})
            save_results = result.get('save_results', {})
            db_summary = result.get('database_summary', {})

            print(f"\n📊 수집 결과:")
            print(f"   📁 총 테마: {collection_stats.get('total_themes', 0)}개")
            print(f"   📈 총 종목: {collection_stats.get('total_stocks', 0)}개")

            print(f"\n🔄 누락 데이터 현황:")
            print(f"   📊 확인 종목: {missing_stats.get('total_stocks', 0)}개")
            print(f"   ⚠️  누락 거래일: {missing_stats.get('missing_days_found', 0)}개")

            print(f"\n💾 데이터베이스 저장:")
            print(f"   🎯 테마 저장: {'✅ 성공' if save_results.get('theme_data') else '❌ 실패'}")
            print(f"   📈 일봉 저장: {'✅ 성공' if save_results.get('daily_data') else '❌ 실패'}")

            # DB 현황
            if db_summary and 'themes' in db_summary:
                themes_info = db_summary['themes']
                print(f"\n🗄️  데이터베이스 현황:")
                print(f"   📁 저장된 테마: {themes_info.get('total_themes', 0)}개")
                print(f"   🔗 테마-종목 매핑: {themes_info.get('total_mappings', 0)}개")
                print(f"   📊 고유 종목: {themes_info.get('unique_stocks', 0)}개")
                if themes_info.get('last_update'):
                    print(f"   🕐 최종 업데이트: {themes_info['last_update']}")

        print("\n✅ 메인 실행 완료 (DB 저장 포함)")

    except Exception as e:
        print(f"❌ 메인 실행 오류: {e}")
        import traceback

        traceback.print_exc()

    finally:
        if req_query:
            print("🧹 리소스 정리 중...")
            req_query.cleanup()
            print("✅ 리소스 정리 완료")