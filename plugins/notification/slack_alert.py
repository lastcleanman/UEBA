from common.setup.logger import get_logger
logger = get_logger("Plugin-Slack")

def execute(df):
    logger.info("Slack 알람 발송 모듈 (현재는 Bypass)")
    # 나중에 df.toPandas()를 하여 90점 이상인 데이터만 슬랙 API로 전송합니다.
    return df