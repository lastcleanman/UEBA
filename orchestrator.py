import sys
import traceback
import os
import json
import importlib
import urllib.request
import glob
import time
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET
from xml.dom import minidom
from sqlalchemy import create_engine, text

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
PARSER_DIR = "/UEBA/common/parser"
DB_SOURCES_PATH = "/UEBA/common/setup/db_sources.json"

# --- [DB 설정 및 엔진 생성] ---
def get_db_engine_by_name(db_name="ueba_mariaDB"):
    try:
        if not os.path.exists(DB_SOURCES_PATH):
            logger.error(f"❌ DB 설정 파일을 찾을 수 없습니다: {DB_SOURCES_PATH}")
            return None
            
        with open(DB_SOURCES_PATH, "r", encoding="utf-8") as f:
            data_sources = json.load(f)
            
        if isinstance(data_sources, list):
            conf = next((s for s in data_sources if s.get("name") == db_name), None)
        else:
            conf = data_sources if data_sources.get("name") == db_name else None

        if not conf:
            logger.error(f"❌ '{db_name}' 설정을 찾을 수 없습니다.")
            return None
            
        target_db = conf.get('database')
        db_url = f"mysql+pymysql://{conf['user']}:{conf['password']}@{conf['host']}:{conf['port']}/{target_db}"
        return create_engine(db_url, pool_pre_ping=True)
        
    except Exception as e:
        logger.error(f"❌ DB 엔진 생성 실패: {e}")
        return None

db_engine = get_db_engine_by_name("ueba_mariaDB")

# --- [Step 1~3] 자율 학습 및 파서 저장 ---
def auto_learn_and_save_parsers():
    if db_engine is None: return
    
    logger.info("🕵️ [Step 1-3] 신규 패턴 학습 및 파서 업데이트 시작")
    if not os.path.exists(PARSER_DIR): os.makedirs(PARSER_DIR, exist_ok=True)

    log_files = glob.glob("/UEBA/data/logs/*.log")
    inference_map = {
        "user": "user_id", "user_id": "user_id",
        "ip": "src_ip", "src_ip": "src_ip",
        "department": "department", "action": "action",
        "device_id": "device_id", "resource": "resource"
    }

    for file_path in log_files:
        filename = os.path.basename(file_path)
        source_name = "Unknown"
        if "authentication" in filename: source_name = "Auth_Logs"
        elif "webserver" in filename: source_name = "Web_Logs"
        elif "endpoint" in filename: source_name = "Endpoint_Logs"
        elif "firewall" in filename: source_name = "Firewall_Logs"
        
        if source_name == "Unknown": continue

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                line = f.readline().strip()
                if not line: continue
                sample = json.loads(line)
            
            root = ET.Element("parser", name=source_name)
            has_user = False
            for k, v in sample.items():
                if k in inference_map:
                    target = inference_map[k]
                    ET.SubElement(root, "field", target=target, source=k)
                    if target == "user_id": has_user = True
            
            if has_user:
                ET.SubElement(root, "field", target="emp_name", source="mapped_name")

            xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="    ")

            with open(os.path.join(PARSER_DIR, f"{source_name}.xml"), "w", encoding="utf-8") as xf:
                xf.write(xml_str)
            
            with db_engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO sj_ueba_parsers (source_name, parser_xml)
                    VALUES (:source, :xml)
                    ON DUPLICATE KEY UPDATE parser_xml = :xml, updated_at = CURRENT_TIMESTAMP
                """), {"source": source_name, "xml": xml_str})
            logger.info(f"✅ [{source_name}] 파서 동기화 완료")
        except Exception as e:
            logger.error(f"❌ [{source_name}] 학습 실패: {e}")

# --- [Step 4] 수집 이력 저장 함수 ---
def save_history(source, count, status, error="", start_time=None):
    if db_engine is None: return
    try:
        with db_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO sj_ueba_ingestion_history (source_name, processed_count, status, error_message, start_time)
                VALUES (:source, :count, :status, :error, :start)
            """), {
                "source": source, "count": count, "status": status, "error": error, "start": start_time
            })
        logger.info(f"📜 [History] {source} 처리 이력 기록 완료 (건수: {count})")
    except Exception as e:
        logger.warning(f"⚠️ DB 이력 저장 실패: {e}")

# --- [유틸리티 함수] ---
def get_last_ts(source_name):
    try:
        if os.path.exists(WATERMARK_FILE):
            with open(WATERMARK_FILE, "r") as f:
                data = json.load(f)
                return data.get(source_name, "1970-01-01 00:00:00")
    except: pass
    return "1970-01-01 00:00:00"

def set_last_ts(source_name, ts):
    try:
        data = {}
        if os.path.exists(WATERMARK_FILE):
            with open(WATERMARK_FILE, "r") as f: data = json.load(f)
        data[source_name] = str(ts)
        with open(WATERMARK_FILE, "w") as f: json.dump(data, f)
    except: pass

def reset_and_init_es():
    es_url = f"http://{ES_HOST}:{ES_PORT}/{ES_INDEX_NAME}"
    try:
        req_del = urllib.request.Request(es_url, method="DELETE")
        urllib.request.urlopen(req_del)
        logger.info(f"🗑️ 기존 인덱스 삭제: {ES_INDEX_NAME}")
    except: pass
    
    mapping = { "mappings": { "properties": {
        "final_ts": { "type": "date", "format": "yyyy-MM-dd+HH:mm||yyyy-MM-dd||yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis" },
        "log_source": { "type": "keyword" }, "user_id": { "type": "keyword" },
        "action": { "type": "keyword" }, "risk_score": { "type": "double" },
        "emp_name": { "type": "keyword" }
    }}}
    try:
        req = urllib.request.Request(es_url, data=json.dumps(mapping).encode("utf-8"), method="PUT")
        req.add_header("Content-Type", "application/json")
        urllib.request.urlopen(req)
        logger.info(f"✅ ES 매핑 초기화 완료")
    except Exception as e: logger.error(f"❌ ES 초기화 실패: {e}")

# --- [핵심 파이프라인 엔진] ---
def run_pipeline(spark, active_plugins):
    auto_learn_and_save_parsers()

    sources = []
    source_files = glob.glob(f"{CONFIG_DIR}/*_sources.json")
    for file_path in source_files:
        with open(file_path, "r", encoding="utf-8") as f:
            sources.extend([s for s in json.load(f) if s.get("enabled", True)])

    total_processed = 0
    for source in sources:
        start_time = datetime.now()
        source_name = source.get('name', 'Unknown')
        
        # ⭐️ [복구됨] 소스별 워터마크 설정값 가져오기
        watermark_col = source.get("watermark_col", "final_ts")
        watermark_default = source.get("watermark_default", "1970-01-01 00:00:00")
        source_type = source.get("type", "").lower()
        
        try:
            last_ts = get_last_ts(source_name)
            
            # ⭐️ [복구됨] 파일에 저장된 이력이 없고 특별한 기본값(예: "0")이 있다면 교체
            if last_ts == "1970-01-01 00:00:00" and watermark_default != "1970-01-01 00:00:00":
                last_ts = watermark_default
                
            # 1. 데이터 가져오기 (DB/파일 소스 공통)
            raw_pandas_df = fetch_data(source, last_updated=last_ts)
            
            # [궁극의 방어선] None이거나, 유령 컬럼만 있는 경우 완벽 차단
            if raw_pandas_df is None or raw_pandas_df.empty or len(raw_pandas_df.index) == 0:
                logger.info(f"⏩ [{source_name}] 신규 수집 데이터가 없습니다. (0건 기록)")
                save_history(source_name, 0, "SUCCESS", start_time=start_time)
                continue

            raw_pandas_df = raw_pandas_df.dropna(axis=1, how='all')
            if raw_pandas_df.empty:
                logger.info(f"⏩ [{source_name}] 유효한 값(알맹이)이 있는 데이터가 없습니다. (0건 기록)")
                save_history(source_name, 0, "SUCCESS", start_time=start_time)
                continue
                
            # ⭐️ [핵심 복구] Spark로 변환하기 전, 원본 DB 데이터에서 기준 컬럼(emp_id 등)의 최댓값을 미리 추출!
            new_max_ts = None
            if watermark_col in raw_pandas_df.columns:
                new_max_ts = str(raw_pandas_df[watermark_col].max())

            # 2. Pandas DataFrame을 딕셔너리 리스트로 변환
            safe_pandas_df = raw_pandas_df.replace({pd.NA: None}).where(pd.notnull(raw_pandas_df), None)
            dict_list = safe_pandas_df.to_dict(orient='records')
            
            if not dict_list or len(dict_list) == 0:
                logger.info(f"⏩ [{source_name}] Spark에 넘길 유효 데이터 행이 없습니다. (0건 기록)")
                save_history(source_name, 0, "SUCCESS", start_time=start_time)
                continue

            # 3. 안전이 확보된 데이터만 Spark DataFrame으로 생성
            spark_df = spark.createDataFrame(dict_list)
            clean_df = normalize_data(spark, spark_df, source_name)
            
            # ⭐️ [복구됨] DB 소스는 쿼리에서 필터링했으므로, 파일 소스일 때만 Spark 메모리 2차 필터링
            if source_type == "file":
                clean_df = clean_df.filter(col("final_ts").cast("string") > lit(str(last_ts)))
                
            current_count = clean_df.count()
            
            if current_count == 0:
                logger.info(f"⏩ [{source_name}] 워터마크 이후 신규 데이터가 없습니다. (0건 기록)")
                save_history(source_name, 0, "SUCCESS", start_time=start_time)
                continue
                
            # 4. 수집 성공 시 Watermark 갱신 (원본 DB에서 구한 emp_id 등의 최댓값을 최우선 사용)
            if new_max_ts:
                set_last_ts(source_name, new_max_ts)
                logger.info(f"📍 [{source_name}] Watermark 갱신 완료 ({watermark_col}): {new_max_ts}")
            else:
                max_ts = clean_df.agg({"final_ts": "max"}).collect()[0][0]
                if max_ts: 
                    set_last_ts(source_name, str(max_ts))
                    logger.info(f"📍 [{source_name}] Watermark 갱신 완료: {max_ts}")

            # 5. 플러그인 실행 및 적재
            detected_df = load_and_run_plugins(clean_df, active_plugins.get("detection", []), "Detection")
            load_and_run_plugins(detected_df, active_plugins.get("loading", []), "Loading")
            
            save_history(source_name, current_count, "SUCCESS", start_time=start_time)
            total_processed += current_count

        except Exception as e:
            logger.error(f"❌ [{source_name}] 파이프라인 실패: {e}")
            save_history(source_name, 0, "FAIL", error=str(e), start_time=start_time)

    return total_processed

def load_and_run_plugins(df, plugin_list, step_name):
    for plugin_path in plugin_list:
        try:
            plugin_module = importlib.import_module(plugin_path)
            if hasattr(plugin_module, "execute"): df = plugin_module.execute(df)
        except Exception as e: logger.error(f"❌ {step_name} 플러그인 {plugin_path} 실패: {e}")
    return df

def main():
    logger.info("🚀 UEBA 자율 주행 수집 엔진 가동")
    reset_and_init_es()
    
    spark = get_spark_session()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    
    with open("/UEBA/pipeline_config.json", "r") as f:
        config = json.load(f)
    active_plugins = config.get("active_plugins", {})

    try:
        while True:
            logger.info(f"\n--- {datetime.now()} 수집 주기 시작 ---")
            count = run_pipeline(spark, active_plugins)
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("🛑 수집 엔진을 종료합니다.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()