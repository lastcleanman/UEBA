from common.ingestion.rdbms_connector import RDBMSConnector
from common.ingestion.file_connector import FileConnector
from common.setup.logger import get_logger

logger = get_logger("Ingestion")

def fetch_data(config):
    source_name = config.get("name", "Unknown")
    source_type = config.get("type").lower()
    
    try:
        # [통일] postgres 인식 추가
        if source_type in ["postgresql", "postgres", "mysql", "mariadb"]:
            connector = RDBMSConnector(config)
            return connector.fetch()
        elif source_type == "file":
            # [통일] FileConnector 명칭 사용
            connector = FileConnector(config)
            return connector.fetch()
        else:
            logger.error(f"❌ 지원하지 않는 타입: {source_type}")
            return None
    except Exception as e:
        logger.error(f"❌ [{source_name}] 수집 중 에러: {str(e)}")
        return None