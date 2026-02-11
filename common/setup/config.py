import os

ES_HOST = os.getenv("TARGET_ES_HOST", "ueba-elasticsearch")
ES_PORT = int(os.getenv("TARGET_ES_PORT", 9200))
ES_INDEX_NAME = "ueba-alerts"

# [수정] 단일 파일이 아닌 설정 파일들이 모인 '디렉토리(폴더) 경로'로 변경
CONFIG_DIR = os.getenv("CONFIG_DIR", "/UEBA/common/setup")