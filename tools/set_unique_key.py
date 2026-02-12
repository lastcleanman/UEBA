import pymysql

DB_CONFIG = {
    "host": "192.168.0.131", "port": 13306,
    "user": "ueba_user", "password": "Suju!0901", "db": "UEBA_TEST", "autocommit": True
}

try:
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()
    # 1. 기존 데이터 초기화
    cursor.execute("TRUNCATE TABLE ueba_alerts")
    # 2. 중복을 판단할 4가지 핵심 기둥(시간, 사번, IP, 출처)을 묶어서 유니크 키 설정
    # 이 4개가 똑같은 데이터가 들어오면 DB가 알아서 거절합니다.
    cursor.execute("""
        ALTER TABLE ueba_alerts 
        ADD UNIQUE KEY uq_log_entry (final_ts, emp_id, src_ip, log_source)
    """)
    print("✅ 중복 방지 규칙(Unique Key) 설정 완료!")
    conn.close()
except Exception as e:
    print(f"❌ 설정 실패 (이미 설정되었을 수 있음): {e}")
