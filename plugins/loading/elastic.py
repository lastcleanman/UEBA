import pandas as pd
from pyspark.sql.functions import col, date_format
from common.processing.es_loader import ESLoader  
from common.setup.config import ES_HOST, ES_PORT, ES_INDEX_NAME

def execute(df):
    try:
        # 1. final_ts 필드를 Elasticsearch 표준 포맷(ISO 8601)으로 변환
        if "final_ts" in df.columns:
            # 공백 대신 'T'를 넣어 표준 형식을 맞춥니다.
            df = df.withColumn("final_ts", date_format(col("final_ts"), "yyyy-MM-dd'T'HH:mm:ss"))

        # 2. 숫자 필드 안전 변환
        numeric_cols = ["duration", "src_port", "dst_port", "packets_total", "bytes_total", "risk_score"]
        for c in numeric_cols:
            if c in df.columns:
                df = df.withColumn(c, col(c).cast("double"))

        final_pdf = df.toPandas()
        
        # 3. Pandas 결측치 보정
        final_pdf = final_pdf.fillna({c: 0.0 for c in numeric_cols})

        loader = ESLoader(host=ES_HOST, port=ES_PORT)
        success_count = loader.load(final_pdf, index_name=ES_INDEX_NAME)
        
        if success_count > 0:
            from common.setup.logger import get_logger
            get_logger("Plugin-ElasticLoad").info(f"✅ Elasticsearch 적재 성공: {success_count}건")
            
    except Exception as e:
        print(f"❌ 적재 에러 발생: {e}")
    return df