import pymysql

DB_CONFIG = {
    "host": "192.168.0.131",
    "port": 13306,
    "user": "ueba_user",
    "password": "Suju!0901",
    "db": "UEBA_TEST",
    "autocommit": True
}

try:
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    # 테이블 내용만 싹 비우기 (TRUNCATE)
    cursor.execute("TRUNCATE TABLE ueba_alerts")
    print("🗑️  테이블(ueba_alerts) 데이터를 모두 비웠습니다! (0건)")
    conn.close()
except Exception as e:
    print(f"❌ 실패: {e}")
