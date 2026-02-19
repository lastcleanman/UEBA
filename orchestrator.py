import sys
import traceback
import os
import json
import importlib
import urllib.request
import glob
import time  # 실시간 반복을 위해 추가
from datetime import datetime
import pandas as pd
if "/UEBA" not in sys.path:
    sys.path.insert(0, "/UEBA")

from common.setup.spark_manager import get_spark_session
from common.setup.logger import get_logger
from common.setup.config import ES_HOST, ES_PORT, ES_INDEX_NAME, CONFIG_DIR
from common.ingestion.data_reader import fetch_data
from common.processing.normalizer import normalize_data
from pyspark.sql.functions import col, lit

logger = get_logger("Orchestrator")

WATERMARK_FILE = "/UEBA/watermark.json"

def get_last_ts(source_name):
    """마지막으로 수집한 기준 시간을 가져옵니다."""
    try:
        if os.path.exists(WATERMARK_FILE):
            with open(WATERMARK_FILE, "r") as f:
                data = json.load(f)
                return data.get(source_name, "1970-01-01 00:00:00")
    except Exception as e:
        logger.error(f"Watermark 읽기 실패: {e}")
    return "1970-01-01 00:00:00"

def set_last_ts(source_name, ts):
    """새로 수집한 데이터 중 가장 최신 시간을 기록합니다."""
    try:
        data = {}
        if os.path.exists(WATERMARK_FILE):
            with open(WATERMARK_FILE, "r") as f:
                data = json.load(f)
        data[source_name] = str(ts)
        with open(WATERMARK_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Watermark 저장 실패: {e}")

def save_history(source, index, count, status, error=""):
    """파이프라인 실행 완료 시 MariaDB에 이력을 남기는 함수 (최적화 버전)"""
    try:
        from sqlalchemy import create_engine, text
        # DB 연결 설정
        db_url = "mysql+pymysql://ueba_user:Suju!0901@192.168.0.131:13306/UEBA_TEST"
        engine = create_engine(db_url, pool_pre_ping=True)
        
        with engine.begin() as conn:
            # 1. 테이블 생성 (없을 경우에만)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ueba_ingestion_history (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    collect_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                    source_name VARCHAR(50),
                    target_index VARCHAR(50),
                    count INT,
                    status VARCHAR(20),
                    error_message TEXT
                )
            """))
            
            # 2. 이력 삽입 (Named Parameter 방식 사용)
            conn.execute(text("""
                INSERT INTO ueba_ingestion_history (source_name, target_index, count, status, error_message)
                VALUES (:source, :index, :count, :status, :error)
            """), {
                "source": source, 
                "index": index, 
                "count": count, 
                "status": status, 
                "error": error
            })
            
        logger.info(f"📜 [History] {source} -> {count}건 기록 완료")
    except Exception as e:
        logger.warning(f"⚠️ DB 이력 저장 실패: {e}")

def reset_and_init_es():
    """Elasticsearch 인덱스 초기화 (처음 가동 시 1회 권장)"""
    es_url = f"http://{ES_HOST}:{ES_PORT}/{ES_INDEX_NAME}"
    try:
        req_del = urllib.request.Request(es_url, method="DELETE")
        urllib.request.urlopen(req_del)
        logger.info(f"🗑️ 기존 인덱스 삭제: {ES_INDEX_NAME}")
    except: pass
    
    mapping = { "mappings": { "properties": {
        "final_ts": { 
            "type": "date", 
            # yyyy-MM-dd+HH:mm 형태를 명시적으로 지원하도록 수정
            "format": "yyyy-MM-dd+HH:mm||yyyy-MM-dd||yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd HH:mm:ss||yyyy-MM-ddZ||strict_date_optional_time||epoch_millis" 
        },
        "log_source": { "type": "keyword" },
        "user_id": { "type": "keyword" },
        "action": { "type": "keyword" },
        "risk_score": { "type": "double" }
    }}}
    
    try:
        req = urllib.request.Request(es_url, data=json.dumps(mapping).encode("utf-8"), method="PUT")
        req.add_header("Content-Type", "application/json")
        urllib.request.urlopen(req)
        logger.info(f"✅ ES 매핑 초기화 완료")
    except Exception as e:
        logger.error(f"❌ ES 초기화 실패: {e}")

def run_pipeline(spark, active_plugins):
    """실제 수집 및 분석 프로세스 (반복 실행됨)"""
    # 1. 설정된 모든 소스 파일 읽기 (*_sources.json)
    sources = []
    source_files = glob.glob(f"{CONFIG_DIR}/*_sources.json")
    
    for file_path in source_files:
        with open(file_path, "r", encoding="utf-8") as f:
            file_sources = json.load(f)
            sources.extend([s for s in file_sources if s.get("enabled", True)])

    total_processed = 0
    for source in sources:
        # ueba-webserver에서 넘어온 날짜별 와일드카드 경로 처리 포함
        raw_pandas_df = fetch_data(source)
        
        if raw_pandas_df is None or raw_pandas_df.empty:
            continue
        
        source_name = source.get('name', 'Unknown')
        
        try:
            # ⭐️ [핵심 수정]: Pandas DF를 PySpark가 싫어하는 Arrow 방식 없이 안전하게 Spark DF로 변환
            # 1. NaN, NaT 값을 None으로 치환 (변환 오류 방지)
            safe_pandas_df = raw_pandas_df.replace({pd.NA: None}).where(pd.notnull(raw_pandas_df), None)
            
            # 2. DataFrame을 Dictionary List 형식으로 풀어버림
            dict_list = safe_pandas_df.to_dict(orient='records')
            
            # 3. 풀어진 리스트를 바탕으로 깨끗한 Spark DataFrame 생성
            spark_df = spark.createDataFrame(dict_list)
            
        except Exception as e:
            logger.error(f"❌ [{source_name}] PySpark 변환 실패. 원본 데이터 구조를 확인하세요: {e}")
            continue

        # 정규화 및 플러그인 실행
        clean_df = normalize_data(spark, spark_df, source_name)
        
        # ⭐️ [증분 수집 로직: 강력 버전] ⭐️
        last_ts = get_last_ts(source_name)
        logger.info(f"🔍 [{source_name}] 저장된 Watermark: {last_ts}")
        
        # Spark 전용 함수를 사용해 확실하게 비교 (문자열로 강제 변환 후 크기 비교)
        clean_df = clean_df.filter(col("final_ts").cast("string") > lit(str(last_ts)))
        
        current_count = clean_df.count()
        
        if current_count == 0:
            logger.info(f"⏩ [{source_name}] 새 데이터가 없습니다. 건너뜁니다.")
            continue
            
        # 새 데이터 중 가장 최신 시간 찾아서 저장
        max_row = clean_df.agg({"final_ts": "max"}).collect()[0]
        max_ts = max_row[0] if max_row else None
        
        if max_ts:
            set_last_ts(source_name, max_ts)
            logger.info(f"💾 [{source_name}] Watermark 갱신 완료: {max_ts}")
        # ⭐️ [증분 수집 로직 끝] ⭐️

        # 플러그인 처리 및 이력 저장
        detected_df = load_and_run_plugins(clean_df, active_plugins.get("detection", []), "Detection")
        load_and_run_plugins(detected_df, active_plugins.get("loading", []), "Loading")
        load_and_run_plugins(detected_df, active_plugins.get("notification", []), "Notification")
        
        save_history(source_name, ES_INDEX_NAME, current_count, "SUCCESS")
        total_processed += current_count

    if total_processed > 0:
        logger.info(f"--- 처리 완료 ({total_processed}건) / DB 이력 기록 안함(테스트용) ---")
        
    return total_processed

def load_and_run_plugins(df, plugin_list, step_name):
    for plugin_path in plugin_list:
        try:
            plugin_module = importlib.import_module(plugin_path)
            if hasattr(plugin_module, "execute"):
                df = plugin_module.execute(df)
        except Exception as e:
            logger.error(f"❌ {step_name} 플러그인 {plugin_path} 실패: {e}")
    return df

def main():
    logger.info("🚀 UEBA 실시간 원격 수집 엔진 시작")
    
    # 처음 실행 시 한 번 인덱스 정리 (필요에 따라 주석 처리)
    reset_and_init_es()
    
    spark = get_spark_session()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
    config_path = "/UEBA/pipeline_config.json"
    with open(config_path, "r") as f:
        config = json.load(f)
    active_plugins = config.get("active_plugins", {})

    try:
        while True:
            logger.info(f"\n--- {datetime.now()} 수집 주기 시작 ---")
            count = run_pipeline(spark, active_plugins)
            logger.info(f"--- 처리 완료 ({count}건) / 30초 대기 ---")
            time.sleep(30) # 10초마다 새 로그 체크
    except KeyboardInterrupt:
        logger.info("🛑 수집 엔진을 종료합니다.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()