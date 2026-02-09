import pandas as pd
from sqlalchemy import create_engine
import urllib.parse

class RDBMSConnector:
    def __init__(self, config):
        self.config = config
        self.engine = self._create_engine()

    def _create_engine(self):
        uri = self._get_db_uri()
        return create_engine(uri)

    def _get_db_uri(self):
        db_type = self.config.get('type', 'postgres').lower()
        user = self.config['user']
        password = urllib.parse.quote_plus(self.config['password'])
        host = self.config['host']
        port = self.config['port']
        dbname = self.config.get('dbname', '')

        if db_type == 'postgres':
            return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        
        elif db_type in ['mysql', 'mariadb']:
            return f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}"
        
        elif db_type == 'oracle':
            # Oracle은 dbname을 service_name으로 처리
            service_name = self.config.get('service_name', dbname)
            return f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service_name}"
        
        elif db_type == 'mssql':
            return f"mssql+pymssql://{user}:{password}@{host}:{port}/{dbname}"
        
        else:
            raise ValueError(f"지원하지 않는 DB 타입입니다: {db_type}")

    def fetch(self, query):
        """Python 엔진으로 쿼리 실행 후 Pandas DataFrame 반환"""
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql(query, connection)
            return df
        except Exception as e:
            print(f"[{self.config.get('type')}] Connection Error: {e}")
            return pd.DataFrame()