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
        
        self.database = (config.get("database") or 
                         config.get("dbname") or 
                         config.get("db_name") or 
                         config.get("db") or 
                         config.get("schema"))
                         
        # ⭐️ [고도화] 테이블명과 워터마크 기준 컬럼 변수 매핑
        self.table_name = config.get("table_name")
        self.watermark_col = config.get("watermark_col")
        self.last_updated = config.get("last_updated", "1970-01-01 00:00:00")
        
        # 특수 목적용 하드코딩 쿼리 (HR 마스터 등 예외 상황을 위한 훌륭한 대비책)
        self.query = config.get("query")

    def fetch(self):
        if not self.database:
            logger.error(f"❌ [{self.source_name}] DB 이름이 설정되지 않았습니다.")
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

            engine = create_engine(url, pool_pre_ping=True)
            
            # ⭐️ [핵심 엔진] 쿼리가 비어있다면, 테이블명과 워터마크 컬럼을 이용해 자동 생성!
            if not self.query:
                if not self.table_name:
                    logger.error(f"❌ [{self.source_name}] JSON 설정에 'table_name' 또는 'query'가 필요합니다.")
                    return None
                
                if self.watermark_col:
                    # 동적 증분 수집 쿼리 생성
                    self.query = f"SELECT * FROM {self.table_name} WHERE {self.watermark_col} > :last_updated"
                else:
                    # 키값이 없으면 전체 수집
                    self.query = f"SELECT * FROM {self.table_name}"

            # DB 연결 및 데이터 추출
            with engine.connect() as conn:
                # 쿼리문에 :last_updated 바인딩 변수가 있을 때만 params 전달 (에러 방지)
                if ":last_updated" in self.query:
                    df = pd.read_sql(
                        text(self.query), 
                        conn, 
                        params={"last_updated": self.last_updated}
                    )
                else:
                    df = pd.read_sql(text(self.query), conn)
            
            # 로그에 타겟 테이블명도 함께 출력하여 디버깅을 편하게 만듭니다.
            target_info = self.table_name if self.table_name else "Custom Query"
            logger.info(f"✅ [{self.source_name}] 추출 성공 ({len(df)}건) / 테이블: {target_info} / 기준: {self.last_updated}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ 데이터 추출 실패 ({self.db_type}): {e}")
            logger.error(f"   - 실행된 쿼리: {self.query}")
            return None