from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_format, coalesce, when, current_timestamp, hour, dayofweek
from pyspark.sql.types import DoubleType
from src.connectors.file_connector import FileLogConnector
from src.connectors.rdb_connector import RDBMSConnector
from src.loaders.es_loader import ESLoader
import sys, os, json, traceback
import urllib.request, urllib.error
import pandas as pd
import numpy as np

# [설계서 Ⅱ-1] 프로젝트 루트 경로 고정
PROJECT_ROOT = "/UEBA"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# -------------------------------------------------------------------------
# [INIT] Elasticsearch 인덱스 및 매핑 초기화 함수
# -------------------------------------------------------------------------
def initialize_es_index(host, port, index_name):
    es_url = f"http://{host}:{port}/{index_name}"
    
    mapping_body = {
        "mappings": {
            "properties": {
                "timestamp": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis" },
                "log_source": { "type": "keyword" },
                "user_id": { "type": "keyword" },
                "action": { "type": "keyword" },
                "src_ip": { "type": "keyword" },
                "department": { "type": "keyword" },
                "salary": { "type": "double" },
                "risk_score": { "type": "double" },
                "alert_message": { "type": "keyword" }
            }
        }
    }

    try:
        req = urllib.request.Request(es_url, method='HEAD')
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                print(f">>> [INIT] 인덱스 '{index_name}'이 이미 존재합니다. 매핑 설정을 건너뜁니다.")
                return

    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f">>> [INIT] 인덱스 '{index_name}' 생성 및 매핑 적용 시작...")
            json_data = json.dumps(mapping_body).encode('utf-8')
            req = urllib.request.Request(es_url, data=json_data, method='PUT')
            req.add_header('Content-Type', 'application/json')
            try:
                with urllib.request.urlopen(req) as response:
                    if response.status == 200:
                        print(f">>> [SUCCESS] 인덱스 매핑 적용 완료!")
            except urllib.error.HTTPError as put_err:
                print(f">>> [WARNING] 인덱스 생성 실패: {put_err.read().decode()}")

# -------------------------------------------------------------------------

def process_single_source(source_config, spark, es_loader):
    source_name = source_config.get('name', 'Unknown')
    source_type = source_config.get('type', 'unknown').lower()
    
    print(f"\n>>> [START] '{source_name}' ({source_type}) 처리 시작")

    try:
        # [1] 데이터 수집
        raw_pandas_df = None
        if source_type in ['mariadb', 'mysql', 'postgresql', 'postgres', 'oracle']:
            db = RDBMSConnector({
                "type": source_config['type'], "host": source_config['host'],
                "port": source_config['port'], "user": source_config['user'],
                "password": source_config['password'], "dbname": source_config['db_name']
            })
            raw_pandas_df = db.fetch(source_config['query'])
        elif source_type == 'file': 
            raw_pandas_df = FileLogConnector({"path": source_config['path']}).fetch()

        if raw_pandas_df is None or raw_pandas_df.empty:
            print(f"   - [SKIP] 데이터 없음")
            return

        print(f"   - 수집 완료: {len(raw_pandas_df)} 건")

        # [Type Fix] Raw 데이터 문자열 변환
        try:
            raw_pandas_df = raw_pandas_df.astype(str)
            raw_pandas_df = raw_pandas_df.replace({'nan': '', 'None': '', 'NaT': ''})
        except:
            pass

        # [2] Spark 가공
        spark_df = spark.createDataFrame(raw_pandas_df)
        
        # 필수 컬럼 생성
        required_cols = ["user_id", "employee_id", "name", "name_kr", "action", "log_source", "src_ip", "department"]
        for c in required_cols:
            if c not in spark_df.columns:
                spark_df = spark_df.withColumn(c, lit(None).cast("string"))

        if "salary" not in spark_df.columns:
             spark_df = spark_df.withColumn("salary", lit(0.0))

        # 식별자 통합
        spark_df = spark_df.withColumn("user_id", coalesce(
            col("user_id"), col("employee_id"), col("name"), col("name_kr"), lit("Unknown_User")
        ))

        # 기본값 채우기
        spark_df = spark_df.fillna({
            "action": "access_log", "log_source": source_name,
            "src_ip": "Internal", "department": "Unknown_Dept"
        })
        spark_df = spark_df.withColumn("src_ip", coalesce(col("src_ip"), lit("Internal")))
        spark_df = spark_df.withColumn("department", coalesce(col("department"), lit("Unknown_Dept")))

        # 시간 필드 처리
        time_cols = []
        if "updated_at" in spark_df.columns: time_cols.append(col("updated_at"))
        if "hire_date" in spark_df.columns: time_cols.append(col("hire_date"))
        if "timestamp" in spark_df.columns: time_cols.append(col("timestamp"))
        time_cols.append(current_timestamp())

        spark_df = spark_df.withColumn("final_ts", coalesce(*time_cols))
        spark_df = spark_df.withColumn("timestamp", date_format(col("final_ts"), "yyyy-MM-dd HH:mm:ss"))
        spark_df = spark_df.withColumn("timestamp", coalesce(col("timestamp"), date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")))

        # -------------------------------------------------------------------------
        # [NEW] 일반화된 룰셋 적용 (Generalized Rules)
        # -------------------------------------------------------------------------
        # 1. 시간 파생 변수 생성 (요일, 시간 추출)
        # dayofweek: 1=일요일, 7=토요일
        spark_df = spark_df.withColumn("day_of_week", dayofweek(col("timestamp")))
        spark_df = spark_df.withColumn("hour_of_day", hour(col("timestamp")))

        # 2. 조건 정의
        is_weekend = col("day_of_week").isin([1, 7]) # 토, 일
        is_night = (col("hour_of_day") < 9) | (col("hour_of_day") > 19) # 9시 전, 19시 후
        is_off_hours = is_weekend | is_night  # 비업무 시간
        
        is_sensitive_action = col("action").isin(["db_access", "file_download"]) # 민감 행위

        # 3. 위험 점수 및 메시지 자동 할당 (우선순위 기반)
        spark_df = spark_df.withColumn("risk_score", 
            when(col("user_id").startswith("GH_"), 100.0)  # 1순위: 유령 계정 (무조건 위험)
            .when(col("action") == "sudo_exec", 90.0)      # 2순위: 관리자 권한 (매우 위험)
            .when(is_off_hours & is_sensitive_action, 85.0) # 3순위: 휴일/심야 데이터 접근 (김영업 시나리오 일반화)
            .when(col("action") == "login_fail", 50.0)     # 4순위: 로그인 실패
            .otherwise(10.0)                               # 5순위: 정상
        )

        spark_df = spark_df.withColumn("alert_message", 
            when(col("user_id").startswith("GH_"), "Critical: 비인가 계정(Ghost) 생성 탐지") 
            .when(col("action") == "sudo_exec", "Critical: 관리자 권한(Root) 상승 시도")
            .when(is_off_hours & is_sensitive_action, "Warning: 비업무 시간(휴일/심야) 대량 데이터 접근") # [일반화된 메시지]
            .when(col("action") == "login_fail", "Warning: 반복적인 로그인 실패 감지")
            .otherwise("Info: 정상 업무 활동")
        )
        # -------------------------------------------------------------------------

        # Salary 숫자 변환
        spark_df = spark_df.withColumn("salary", col("salary").cast("double"))
        spark_df = spark_df.fillna(0.0, subset=["salary"])

        final_pandas_df = spark_df.toPandas()
        
        # Pandas Safety
        final_pandas_df['salary'] = final_pandas_df['salary'].fillna(0.0)
        final_pandas_df['risk_score'] = final_pandas_df['risk_score'].fillna(0.0)
        if 'alert_message' in final_pandas_df.columns:
            final_pandas_df['alert_message'] = final_pandas_df['alert_message'].fillna("Info: 정상")

        # [3] 적재
        success_count = es_loader.load(
            final_pandas_df, 
            index_name="ueba-alerts", 
            id_cols=[]  
        )
        
        if success_count == len(final_pandas_df):
            print(f"   - [SUCCESS] 적재 완료: {success_count} 건")
        else:
            print(f"   - !!! [ES_ALERT] 일부 실패 ({success_count}/{len(final_pandas_df)})")

    except Exception as e:
        print(f"   - !!! [ERROR] '{source_name}' 처리 중 오류: {e}")
        traceback.print_exc()

def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("UEBA_Integrated_Pipeline_Final") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()

    es_host = os.getenv("TARGET_ES_HOST", "ueba-elasticsearch")
    es_port = int(os.getenv("TARGET_ES_PORT", 9200))
    
    # [RESET] 로직 변경 반영을 위해 인덱스 초기화
    try:
        del_url = f"http://{es_host}:{es_port}/ueba-alerts"
        req = urllib.request.Request(del_url, method='DELETE')
        urllib.request.urlopen(req)
        print(">>> [RESET] 기존 인덱스를 삭제했습니다.")
    except:
        pass

    initialize_es_index(es_host, es_port, "ueba-alerts")

    config_path = os.getenv("CONFIG_FILE_PATH", "/UEBA/config/db_sources.json")
    with open(config_path, 'r', encoding='utf-8') as f:
        sources = json.load(f)

    es_loader = ESLoader(host=es_host, port=es_port)

    print(f">>> 총 {len(sources)}개의 소스 처리를 시작합니다.")
    for source in sources:
        process_single_source(source, spark, es_loader)

    spark.stop()
    print("\n>>> 모든 파이프라인 처리가 완료되었습니다.")

if __name__ == "__main__":
    main()