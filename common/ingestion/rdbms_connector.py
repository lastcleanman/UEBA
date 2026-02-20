import pandas as pd
from sqlalchemy import create_engine, text
from common.setup.logger import get_logger

logger = get_logger("RDBMSConnector")

class RDBMSConnector:
    def __init__(self, config):
        self.config = config
        self.source_name = config.get("name", "Unknown")
        self.db_type = config.get("type", "").lower()
        self.host = config.get("host")
        self.port = config.get("port")
        self.user = config.get("user")
        self.password = config.get("password")
        
        # ⭐️ 모든 가능한 키워드를 총동원하여 데이터베이스 이름을 완벽하게 찾습니다.
        self.database = (config.get("database") or 
                         config.get("dbname") or 
                         config.get("db_name") or 
                         config.get("db") or 
                         config.get("schema"))
                         
        self.query = config.get("query")

    def fetch(self):
        if not self.database:
            logger.error(f"❌ [{self.source_name}] DB 이름이 설정되지 않았습니다. (설정값: {self.config})")
            return None

        try:
            # SQLAlchemy 연결 URL 조합
            if self.db_type in ["postgresql", "postgres"]:
                url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            elif self.db_type in ["mysql", "mariadb"]:
                url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            else:
                logger.error(f"❌ [{self.source_name}] 지원하지 않는 DB 타입입니다: {self.db_type}")
                return None

            # DB 연결 및 데이터 추출 (pool_pre_ping으로 끊어진 연결 자동 방어)
            engine = create_engine(url, pool_pre_ping=True)
            with engine.connect() as conn:
                df = pd.read_sql(text(self.query), conn)
            
            logger.info(f"✅ [{self.source_name}] DB 데이터 추출 성공 ({len(df)}건)")
            return df
            
        except Exception as e:
            logger.error(f"❌ 데이터 추출 실패 ({self.db_type}): {e}")
            logger.error(f"   - 실행 쿼리: {self.query}")
            return None