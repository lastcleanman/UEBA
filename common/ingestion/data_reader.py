import pandas as pd
from common.ingestion.rdbms_connector import RDBMSConnector
from common.ingestion.file_connector import FileConnector
from common.setup.logger import get_logger
import json
import glob
import os

logger = get_logger("Ingestion")

def get_hr_lookup():
    """MariaDB에서 사원 번호와 이름을 매핑하기 위한 딕셔너리 생성"""
    try:
        with open("/UEBA/common/setup/db_sources.json", "r", encoding="utf-8") as f:
            sources = json.load(f)
        
        maria_conf = next((s for s in sources if s["name"] == "ueba_mariaDB"), None)
        if maria_conf and maria_conf.get("enabled"):
            connector = RDBMSConnector(maria_conf)
            hr_df = connector.fetch()
            
            if hr_df is not None and not hr_df.empty:
                hr_df.columns = [c.lower().strip() for c in hr_df.columns]
                id_col = 'emp_id' if 'emp_id' in hr_df.columns else hr_df.columns[0]
                name_col = 'emp_name' if 'emp_name' in hr_df.columns else hr_df.columns[1]
                
                lookup = dict(zip(hr_df[id_col].astype(str), hr_df[name_col].astype(str)))
                logger.info(f"✅ HR 마스터 로드 성공: {len(lookup)}명 매핑 준비 완료")
                return lookup
    except Exception as e:
        logger.warning(f"⚠️ HR 마스터 로드 실패: {e}")
    return None

def fetch_data(config):
    source_name = config.get("name", "Unknown")
    source_type = config.get("type").lower()
    
    try:
        df = None
        if source_type in ["postgresql", "postgres", "mysql", "mariadb"]:
            connector = RDBMSConnector(config)
            df = connector.fetch()
            
        elif source_type == "file":
            path_pattern = config.get("path")
            # [수정] glob을 사용하여 와일드카드 경로에 해당하는 실제 파일들을 모두 찾음
            file_list = glob.glob(path_pattern)
            
            if not file_list:
                logger.error(f"❌ [{source_name}] 파일을 찾을 수 없습니다: {path_pattern}")
                # 디버깅을 위해 상위 디렉토리 상태 확인 로그 추가
                base_path = "/UEBA/data/remote_logs"
                if os.path.exists(base_path):
                    logger.info(f"🔍 [디버깅] {base_path} 내부 폴더 목록: {os.listdir(base_path)}")
                return None

            logger.info(f"📂 [{source_name}] 수집 대상 파일 발견: {len(file_list)}개")
            
            # 여러 개의 파일을 하나로 통합하여 읽기
            df_list = []
            for file_path in file_list:
                # 개별 파일 처리를 위해 임시 설정 생성
                temp_config = config.copy()
                temp_config['path'] = file_path
                connector = FileConnector(temp_config)
                temp_df = connector.fetch()
                if temp_df is not None and not temp_df.empty:
                    df_list.append(temp_df)
            
            if df_list:
                df = pd.concat(df_list, ignore_index=True)

        # 수집된 데이터가 있을 경우 HR 매핑 처리
        if df is not None and not df.empty:
            hr_lookup = get_hr_lookup()
            
            if "user_id" in df.columns:
                if hr_lookup:
                    if "emp_id" not in df.columns:
                        df["emp_id"] = df["user_id"]
                    
                    df['user_id'] = df['user_id'].astype(str).map(hr_lookup).fillna(df['user_id'])
                    
                    # 샘플 로깅
                    sample_user = df['user_id'].iloc[0]
                    logger.info(f"✨ [{source_name}] 매핑 완료 (샘플: {sample_user})")
                else:
                    df['user_id'] = df['user_id'].apply(
                        lambda x: f"가상유저_{str(x)[-3:]}" if str(x).startswith("EMP") else x
                    )
        return df

    except Exception as e:
        logger.error(f"❌ [{source_name}] 수집 중 에러: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None