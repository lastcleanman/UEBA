import pymysql
import sys

# 1. 접속 정보 (외부 IP 사용)
DB_CONFIG = {
    "host": "192.168.0.131",
    "port": 13306,
    "user": "ueba_user",
    "password": "Suju!0901",
    "db": "UEBA_TEST",
    "autocommit": True
}

print("🔄 MariaDB 테이블 초기화를 시작합니다...")

try:
    conn = pymysql.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # 2. 기존 꼬인 테이블 삭제
    cursor.execute("DROP TABLE IF EXISTS ueba_alerts")
    print("🗑️  기존 테이블(ueba_alerts) 삭제 완료")

    # 3. 새 테이블 생성 (Spark 데이터 스키마와 100% 일치)
    sql = """
    CREATE TABLE ueba_alerts (
        final_ts DATETIME,
        log_source VARCHAR(100),
        user_id VARCHAR(100),
        department VARCHAR(100),
        src_ip VARCHAR(50),
        action VARCHAR(100),
        risk_score DOUBLE,
        alert_message TEXT,
        salary DOUBLE,
        
        -- 방화벽 로그 필드
        bytes_total BIGINT,
        packets_total BIGINT,
        machine_name VARCHAR(100),
        fw_rule_id VARCHAR(50),
        src_port INT,
        dst_ip VARCHAR(50),
        dst_port INT,
        protocol VARCHAR(50),
        app_name VARCHAR(100),
        duration INT,
        end_time VARCHAR(50),
        timestamp VARCHAR(50),
        
        -- 인사 정보 필드 (수정됨: hq_code 삭제, emp_id 추가)
        hire_date VARCHAR(50),
        email VARCHAR(100),
        emp_id VARCHAR(50),
        emp_name VARCHAR(50),
        job_title VARCHAR(100),
        phone VARCHAR(50),
        rank_name VARCHAR(50),
        ssn VARCHAR(50),
        static_ip VARCHAR(50),
        
        created_at DATETIME DEFAULT NOW()
    )
    """
    cursor.execute(sql)
    print("✅  새 테이블 생성 완료! (스키마 동기화됨)")
    
    conn.close()

except Exception as e:
    print(f"❌ 실패: {e}")
    sys.exit(1)