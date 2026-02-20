import pandas as pd
from common.ingestion.rdbms_connector import RDBMSConnector
from common.ingestion.file_connector import FileConnector
from common.setup.logger import get_logger
import json
import glob
import os
import xml.etree.ElementTree as ET

logger = get_logger("Ingestion")

def get_hr_lookup():
    """MariaDB에서 사원 번호와 이름을 매핑하기 위한 딕셔너리 생성"""
    try:
        with open("/UEBA/common/setup/db_sources.json", "r", encoding="utf-8") as f:
            sources = json.load(f)
        
        # ⭐️ ueba_mariaDB 설정을 찾음
        target_conf = next((s for s in sources if s["name"] == "ueba_mariaDB"), None)
        if not target_conf: return None
        
        maria_conf = target_conf.copy()
        
        if maria_conf and maria_conf.get("enabled"):
            # ⭐️ RDBMSConnector 호환을 위해 database 키를 dbname으로 복사
            if "database" in maria_conf:
                maria_conf["dbname"] = maria_conf["database"]
            
            connector = RDBMSConnector(maria_conf)
            hr_df = connector.fetch()
            
            if hr_df is not None and not hr_df.empty:
                hr_df.columns = [c.lower().strip() for c in hr_df.columns]
                id_col = 'employee_id' if 'employee_id' in hr_df.columns else ('emp_id' if 'emp_id' in hr_df.columns else hr_df.columns[0])
                name_col = 'name_kr' if 'name_kr' in hr_df.columns else ('emp_name' if 'emp_name' in hr_df.columns else hr_df.columns[1])
                
                lookup = dict(zip(hr_df[id_col].astype(str), hr_df[name_col].astype(str)))
                logger.info(f"✅ HR 마스터 로드 성공: {len(lookup)}명 매핑 준비 완료")
                return lookup
    except Exception as e:
        logger.warning(f"⚠️ HR 마스터 로드 실패: {e}")
    return None

def get_source_field(source_name, target_field, base_dir="/UEBA/common/parser/"):
    xml_path = os.path.join(base_dir, f"{source_name}.xml")
    if not os.path.exists(xml_path):
        return target_field
        
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        for field in root.iter('field'):
            if field.get('target') == target_field:
                return field.get('source')
    except Exception as e:
        logger.error(f"❌ [{source_name}] XML 파싱 에러: {e}")
    return target_field

def fetch_data(config):
    source_name = config.get("name", "Unknown")
    source_type = config.get("type").lower()
    
    try:
        df = None
        if source_type in ["postgresql", "postgres", "mysql", "mariadb"]:
            # ⭐️ 개별 DB 수집 시에도 dbname 키 보정 (None 에러 방지 핵심)
            db_conf = config.copy()
            if "database" in db_conf:
                db_conf["dbname"] = db_conf["database"]
                
            connector = RDBMSConnector(db_conf)
            df = connector.fetch()
            
        elif source_type == "file":
            path_pattern = config.get("path")
            file_list = glob.glob(path_pattern)
            
            if not file_list:
                return None

            df_list = []
            for file_path in file_list:
                temp_config = config.copy()
                temp_config['path'] = file_path
                connector = FileConnector(temp_config)
                temp_df = connector.fetch()
                if temp_df is not None and not temp_df.empty:
                    df_list.append(temp_df)
            
            if df_list:
                df = pd.concat(df_list, ignore_index=True)

        if df is not None and not df.empty:
            hr_lookup = get_hr_lookup()
            
            if hr_lookup:
                src_uid_col = get_source_field(source_name, "user_id")
                src_user_col = get_source_field(source_name, "emp_name") # 차트용 emp_name 기준
                
                if src_uid_col in df.columns:
                    if src_user_col not in df.columns:
                        df[src_user_col] = df[src_uid_col].astype(str).map(hr_lookup)
                    else:
                        df[src_user_col] = df[src_user_col].fillna(df[src_uid_col].astype(str).map(hr_lookup))
                
        return df

    except Exception as e:
        logger.error(f"❌ [{source_name}] 수집 중 에러: {str(e)}")
        return None