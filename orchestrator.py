import sys
import traceback  # [추가] 상세 로그 출력을 위한 모듈
if "/UEBA" not in sys.path:
    sys.path.insert(0, "/UEBA")
import os
import json
import importlib
import urllib.request
import glob
from common.setup.spark_manager import get_spark_session
from common.setup.logger import get_logger
from common.setup.config import ES_HOST, ES_PORT, ES_INDEX_NAME, CONFIG_DIR
from common.ingestion.data_reader import fetch_data
from common.processing.normalizer import normalize_data

logger = get_logger("Orchestrator")

def reset_and_init_es():
    """Elasticsearch 인덱스 초기화 및 매핑 설정"""
    # URL 생성 로그 추가
    es_url = f"http://{ES_HOST}:{ES_PORT}/{ES_INDEX_NAME}"
    logger.info(f"🔍 Elasticsearch 접속 시도: {es_url}")

    try:
        req_del = urllib.request.Request(es_url, method="DELETE")
        urllib.request.urlopen(req_del)
        logger.info(f"🗑️ 기존 인덱스 삭제 완료: {ES_INDEX_NAME}")
    except Exception as e:
        # 삭제 실패는 인덱스가 없을 수도 있으므로 경고만 하고 넘어감 (상세 로그는 생략 가능)
        logger.warning(f"⚠️ 기존 인덱스 삭제 건너뜀 (없거나 접속 실패): {e}")
    
    mapping = { "mappings": { "properties": {
        "final_ts": { "type": "date", "format": "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis" },
        "log_source": { "type": "keyword" }, "user_id": { "type": "keyword" }, "action": { "type": "keyword" },
        "src_ip": { "type": "keyword" }, "department": { "type": "keyword" }, "salary": { "type": "double" },
        "risk_score": { "type": "double" }, "alert_message": { "type": "keyword" }
    }}}
    
    try:
        req = urllib.request.Request(es_url, data=json.dumps(mapping).encode("utf-8"), method="PUT")
        req.add_header("Content-Type", "application/json")
        urllib.request.urlopen(req)
        logger.info(f"✅ 인덱스 매핑 초기화 성공: {ES_INDEX_NAME}")
    except Exception as e:
        logger.error("❌ 인덱스 초기화 치명적 오류 발생!")
        logger.error(f"에러 메시지: {e}")
        logger.error("👇 아래 상세 로그(Traceback)를 확인하세요 👇")
        traceback.print_exc()  # [핵심] 여기서 에러의 뿌리를 보여줍니다.

def load_and_run_plugins(df, plugin_list, step_name):
    """플러그인 동적 로드 및 실행"""
    for plugin_path in plugin_list:
        logger.info(f"[{step_name}] 플러그인 가동: {plugin_path}")
        try:
            plugin_module = importlib.import_module(plugin_path)
            if hasattr(plugin_module, "execute"):
                df = plugin_module.execute(df)
            else:
                logger.error(f"플러그인 {plugin_path}에 'execute' 함수가 없습니다.")
        except Exception as e:
            logger.error(f"플러그인 {plugin_path} 실행 실패")
            traceback.print_exc() # 플러그인 에러도 상세히 출력
    return df

def main():
    logger.info("====== [플러그인 기반] UEBA 파이프라인 가동 ======")
    
    # 설정 파일 내용을 먼저 찍어봅니다 (디버깅용)
    logger.info(f"🔧 현재 설정된 ES 정보: Host={ES_HOST}, Port={ES_PORT}")

    reset_and_init_es()
    spark = get_spark_session()
    
    # 1. 파이프라인 메인 설정 읽기
    config_path = "/UEBA/pipeline_config.json"
    with open(config_path, "r") as f:
        config = json.load(f)
    active_plugins = config.get("active_plugins", {})

    # 2. 모든 설정 파일(*_sources.json) 통합 읽기
    sources = []
    source_files = glob.glob(f"{CONFIG_DIR}/*_sources.json")
    
    for file_path in source_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                file_sources = json.load(f)
                for s in file_sources:
                    if s.get("enabled", True): 
                        sources.append(s)
        except Exception as e:
            logger.error(f"설정 파일 읽기 실패 ({file_path}): {e}")

    # 3. 데이터 스트림 순차 처리
    for source in sources:
        logger.info(f"\n--- 데이터 스트림: {source.get('name')} ---")
        
        raw_pandas_df = fetch_data(source)
        
        if raw_pandas_df is None or raw_pandas_df.empty:
            logger.warning(f"⚠️ [{source.get('name')}] 수집된 데이터가 없어 건너뜁니다.")
            continue
        
        clean_df = normalize_data(spark, raw_pandas_df, source.get('name'))
        detected_df = load_and_run_plugins(clean_df, active_plugins.get("detection", []), "Detection")
        load_and_run_plugins(detected_df, active_plugins.get("loading", []), "Loading")
        load_and_run_plugins(detected_df, active_plugins.get("notification", []), "Notification")

    spark.stop()
    logger.info("====== 모든 파이프라인 처리 완료 ======")

if __name__ == "__main__":
    main()
