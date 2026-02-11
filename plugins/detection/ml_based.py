from common.setup.logger import get_logger
logger = get_logger("Plugin-ML")

def execute(df):
    logger.info("머신러닝 기반 이상징후 탐지 로직 (현재는 Bypass)")
    # 나중에 모델을 로드하여 df에 예측 결과를 추가합니다.
    return df