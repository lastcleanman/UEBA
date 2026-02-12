from pyspark.sql.functions import col, when, lit
from common.setup.logger import get_logger

logger = get_logger("Plugin-RuleEngine")

def execute(df):
    logger.info("탐지 룰셋(Rule-based) 적용 중...")
    
    # 1. 기본 점수 및 메시지 세팅 (기본 10점)
    if "risk_score" not in df.columns:
        df = df.withColumn("risk_score", lit(10.0))
    if "alert_message" not in df.columns:
        df = df.withColumn("alert_message", lit("Info: 정상 업무 활동"))

    # 2. 🚨 [핵심] 강력한 보안 위협 탐지 룰셋 적용
    df = df.withColumn("risk_score",
        when(col("action") == "Reverse_Shell_C2", lit(99.0))
        .when(col("action") == "Massive_FTP_Exfiltration", lit(95.0))
        .when(col("action") == "Unauthorized_DB_Dump", lit(90.0))
        .when(col("action") == "SSH_BruteForce", lit(85.0))
        .otherwise(col("risk_score"))
    )

    df = df.withColumn("alert_message",
        when(col("action") == "Reverse_Shell_C2", lit("🚨 CRITICAL: 랜섬웨어/C2 서버 비인가 통신 감지!"))
        .when(col("action") == "Massive_FTP_Exfiltration", lit("🚨 HIGH: 내부자 대규모 기밀 유출 시도 (FTP)"))
        .when(col("action") == "Unauthorized_DB_Dump", lit("🚨 HIGH: 비인가 DB 덤프 및 데이터 추출 시도"))
        .when(col("action") == "SSH_BruteForce", lit("⚠️ WARN: 무차별 대입 공격 (SSH Brute Force) 도배"))
        .otherwise(col("alert_message"))
    )

    return df
