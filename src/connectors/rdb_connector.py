import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

class RDBMSConnector:
    def __init__(self, config):
        self.config = config
        self.engine = self._create_engine()

    def _create_engine(self):
        """DB 타입에 따른 Connection String 생성"""
        # 대소문자 실수 방지를 위해 소문자로 변환
        db_type = self.config['type'].lower() 
        user = self.config['user']
        password = urllib.parse.quote_plus(self.config['password'])
        host = self.config['host']
        port = self.config['port']
        dbname = self.config['dbname']

        # [수정됨] postgresql, postgres 둘 다 허용하도록 변경
        if db_type in ['mysql', 'mariadb']:
            url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}"
        
        elif db_type in ['postgresql', 'postgres']:  # <--- 여기를 수정했습니다!
            url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        
        elif db_type == 'oracle':
            url = f"oracle+oracledb://{user}:{password}@{host}:{port}/{dbname}"
        elif db_type == 'mssql':
            url = f"mssql+pymssql://{user}:{password}@{host}:{port}/{dbname}"
        else:
            raise ValueError(f"지원하지 않는 DB 타입입니다: {db_type}")
        
        return create_engine(url)

    def fetch(self, query):
        """쿼리 실행 후 Pandas DataFrame 반환"""
        try:
            with self.engine.connect() as connection:
                df = pd.read_sql(text(query), connection)
            return df
        except Exception as e:
            # 에러 로그를 좀 더 자세히 출력
            print(f"!!! [DB Error] 데이터 수집 실패 ({self.config['type']}): {e}")
            return pd.DataFrame()