from pyspark.sql import SparkSession
import sys
import os
import json
import traceback

# [설정] 프로젝트 경로
PROJECT_ROOT = "/UEBA"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.connectors.rdb_connector import RDBMSConnector
from src.processors.spark_processor import SparkDataProcessor
from src.loaders.es_loader import ESLoader

def process_single_source(source_config, spark, es_loader):
    """개별 DB 소스를 처리하는 함수"""
    source_name = source_config.get('name', 'Unknown')
    print(f"\n>>> [START] '{source_name}' ({source_config['type']}) 처리 시작")

    try:
        # 1. DB 접속 설정 (JSON -> Connector 포맷 변환)
        db_conn_config = {
            "type": source_config['type'],
            "host": source_config['host'],
            "port": source_config['port'],
            "user": source_config['user'],
            "password": source_config['password'],
            "dbname": source_config['db_name']
        }
        
        # 2. 데이터 수집
        db = RDBMSConnector(db_conn_config)
        print(f"   - 쿼리 실행: {source_config['query'][:50]}...")
        raw_pandas_df = db.fetch(source_config['query'])

        if raw_pandas_df.empty:
            print(f"   - [SKIP] 수집된 데이터가 없습니다.")
            return

        print(f"   - 수집 완료: {len(raw_pandas_df)} 건")

        # 3. 데이터 가공 (Spark)
        spark_df = spark.createDataFrame(raw_pandas_df)
        processor = SparkDataProcessor(spark)
        processed_spark_df = processor.process_asset_logs(spark_df)
        final_pandas_df = processed_spark_df.toPandas()

        # 4. 데이터 적재 (ES)
        # JSON의 target_index_suffix를 이용해 인덱스명 생성
        index_name = f"ueba-{source_config.get('target_index_suffix', 'logs')}"
        
        # id_fields 파싱 (리스트가 아니라 문자열로 적혀있을 경우 대비)
        id_fields = source_config.get('id_fields', [])
        if isinstance(id_fields, list) and len(id_fields) == 1 and ',' in id_fields[0]:
            # ["col1, col2"] -> ["col1", "col2"] 로 자동 보정
            id_fields = [x.strip() for x in id_fields[0].split(',')]

        success_count = es_loader.load(
            final_pandas_df, 
            index_name=index_name, 
            id_cols=id_fields
        )
        print(f"   - [SUCCESS] 적재 완료: {success_count} 건 (Index: {index_name})")

    except Exception as e:
        print(f"   - !!! [ERROR] '{source_name}' 처리 중 실패: {e}")
        traceback.print_exc()

def main():
    # Spark 세션 생성
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("UEBA_Multi_Source") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

    # 설정 파일 로드
    config_path = os.getenv("CONFIG_FILE_PATH", "/UEBA/configs/db_sources.json")
    
    if not os.path.exists(config_path):
        print(f"!!! 설정 파일을 찾을 수 없습니다: {config_path}")
        return

    with open(config_path, 'r', encoding='utf-8') as f:
        sources = json.load(f)

    # ES 로더 초기화 (공통)
    es_host = os.getenv("TARGET_ES_HOST", "ueba-elasticsearch")
    es_port = int(os.getenv("TARGET_ES_PORT", 9200))
    es_loader = ESLoader(host=es_host, port=es_port)

    print(f">>> 총 {len(sources)}개의 데이터 소스 처리를 시작합니다.")

    # 루프 실행
    for source in sources:
        process_single_source(source, spark, es_loader)

    spark.stop()
    print("\n>>> 모든 파이프라인 처리가 완료되었습니다.")

if __name__ == "__main__":
    main()