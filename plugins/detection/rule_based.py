from pyspark.sql.functions import col, when, hour, dayofweek
from common.setup.logger import get_logger

logger = get_logger("Plugin-RuleEngine")

def execute(df):
    """(필수 인터페이스) 데이터프레임을 받아 처리 후 반환"""
    logger.info("탐지 룰셋(Rule-based) 적용 중...")
    
    df = df.withColumn("day_of_week", dayofweek(col("timestamp")))
    df = df.withColumn("hour_of_day", hour(col("timestamp")))

    is_weekend = col("day_of_week").isin([1, 7])
    is_night = (col("hour_of_day") < 9) | (col("hour_of_day") > 19)
    is_off_hours = is_weekend | is_night
    is_sensitive_action = col("action").isin(["db_access", "file_download"])

    df = df.withColumn("risk_score", 
        when(col("user_id").startswith("GH_"), 100.0)
        .when(col("action") == "sudo_exec", 90.0)
        .when(is_off_hours & is_sensitive_action, 85.0)
        .when(col("action") == "login_fail", 50.0)
        .otherwise(10.0)
    )

    df = df.withColumn("alert_message", 
        when(col("user_id").startswith("GH_"), "Critical: 비인가 계정(Ghost) 생성 탐지") 
        .when(col("action") == "sudo_exec", "Critical: 관리자 권한(Root) 상승 시도")
        .when(is_off_hours & is_sensitive_action, "Warning: 비업무 시간(휴일/심야) 대량 데이터 접근")
        .when(col("action") == "login_fail", "Warning: 반복적인 로그인 실패 감지")
        .otherwise("Info: 정상 업무 활동")
    )
    return df