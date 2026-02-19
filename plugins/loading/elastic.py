import pandas as pd
from pyspark.sql.functions import col, date_format, md5, concat_ws, coalesce, lower, trim, lit, to_timestamp
from common.processing.es_loader import ESLoader  
from common.setup.config import ES_HOST, ES_PORT, ES_INDEX_NAME
from common.setup.logger import get_logger

logger = get_logger("Plugin-ElasticLoad")

def execute(df):
    try:
        # 1. DB 연동 삭제! (현재 데이터프레임의 컬럼만으로 지문 생성)
        # 이제 DB 스키마를 조회하지 않고, 들어온 데이터 자체를 기준으로 중복 방지 ID를 만듭니다.
        actual_cols = [c for c in df.columns if c not in ["row_id", "created_at"]]
        
        hash_targets = []
        for c in actual_cols:
            if c == "final_ts":
                hash_targets.append(date_format(coalesce(col(c), lit("1970-01-01 00:00:00")), "yyyy-MM-dd HH:mm:ss"))
            else:
                hash_targets.append(lower(trim(coalesce(col(c).cast("string"), lit("null")))))
        
        # 고유 row_id 생성
        df = df.withColumn("row_id", md5(concat_ws("|", *hash_targets)))

        # 2. [핵심 수정] final_ts 필드를 완벽하게 인식해서 ISO 8601 포맷으로 강제 변환
        # T 뒤에 한국 시간임을 알리는 +09:00 추가
        if "final_ts" in df.columns:
            from pyspark.sql.functions import regexp_replace
            # 공백을 T로 바꾸고, 맨 뒤에 +09:00을 붙여줍니다.
            df = df.withColumn("final_ts", concat_ws("", regexp_replace(col("final_ts"), " ", "T"), lit("+09:00")))
            
        # 3. 숫자 필드 안전 변환
        numeric_cols = ["duration", "src_port", "dst_port", "packets_total", "bytes_total", "risk_score"]
        for c in numeric_cols:
            if c in df.columns:
                df = df.withColumn(c, col(c).cast("double"))

        # 4. Pandas 변환 및 결측치 보정
        rows = df.collect()
        dict_list = [row.asDict() for row in rows]
        final_pdf = pd.DataFrame(dict_list)
        final_pdf = final_pdf.fillna({c: 0.0 for c in numeric_cols})

        # 5. ES 적재
        loader = ESLoader(host=ES_HOST, port=ES_PORT)
        success_count = loader.load(final_pdf, index_name=ES_INDEX_NAME, id_cols=["row_id"])
        
        if success_count > 0:
            logger.info(f"✅ Elasticsearch 적재 성공: {success_count}건")
            
    except Exception as e:
        logger.error(f"❌ Elasticsearch 적재 중 오류 발생: {e}")
        
    return df, success_count