import os

# 컨테이너 이름 사용
ES_HOST = "ueba-elasticsearch"
ES_PORT = 9200
ES_INDEX_NAME = "ueba-alerts"

# Spark 설정
SPARK_APP_NAME = "UEBA-Pipeline"
SPARK_MASTER = "local[*]"

# [수정됨] 설정 파일들이 위치한 실제 경로 지정
BASE_DIR = "/UEBA"
CONFIG_DIR = os.path.join(BASE_DIR, "common/setup")
