import pandas as pd
import traceback
from sqlalchemy import create_engine, text
from common.ingestion.base import BaseConnector
from common.setup.logger import get_logger

logger = get_logger("RDBMSConnector")

class RDBMSConnector(BaseConnector):
    def __init__(self, config):
        super().__init__(config)
        self.db_type = self.config.get("type", "postgres").lower()
        self.host = self.config.get("host")
        self.port = self.config.get("port")
        self.user = self.config.get("user")
        self.password = self.config.get("password")
        # [통일] pipeline_config.json의 db_name과 일치시킴
        self.dbname = self.config.get("db_name")
        self.engine = self._create_engine()

    def _create_engine(self):
        try:
            if self.db_type in ['postgres', 'postgresql']:
                url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
            elif self.db_type in ['mysql', 'mariadb']:
                url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
            else:
                raise ValueError(f"지원하지 않는 DB 타입: {self.db_type}")
            return create_engine(url, connect_args={'connect_timeout': 5})
        except Exception as e:
            logger.error(f"❌ DB Engine 생성 실패: {str(e)}")
            logger.debug(traceback.format_exc())
            raise e

    def fetch(self, query=None):
        if not query:
            query = self.config.get("query")
        try:
            # SQLAlchemy 2.0 호환성 위해 text() 사용
            df = pd.read_sql(text(query), self.engine)
            return df
        except Exception as e:
            logger.error(f"❌ 데이터 추출 실패 ({self.db_type}): {str(e)}")
            logger.error(f"   - 실행 쿼리: {query}")
            return pd.DataFrame()