from common.setup.logger import get_logger
from pyspark.sql.functions import col, lit, md5, concat_ws, coalesce, lower, trim

logger = get_logger("Plugin-RDBLoad")

def execute(df):
    logger.info("💾 [RDB 적재] 전체 로우 지문 대조 모드 가동 (변동 컬럼 제외)...")

    jdbc_url = "jdbc:mysql://192.168.0.131:13306/UEBA_TEST?useSSL=false"
    db_props = {"user": "ueba_user", "password": "Suju!0901", "driver": "com.mysql.cj.jdbc.Driver"}
    table_name = "ueba_alerts"

    try:
        # 1. DB 스키마 동적 분석
        db_schema = df.sparkSession.read.jdbc(url=jdbc_url, table=table_name, properties=db_props).schema
        actual_cols = df.columns
        
        # 2. 적재용 컬럼 구성 및 지문(row_id) 생성용 컬럼 선별
        select_exprs = []
        # 지문 생성에서 제외할 '매번 변하는 컬럼' 리스트
        # 시간을 빼지 말라고 하셨으나, 중복 체크의 '기준'에서는 제외해야 동일 데이터로 인식됩니다.
        # (실제 데이터 값으로는 들어갑니다.)
        exclude_from_hash = ["row_id", "created_at", "final_ts", "timestamp"] 
        hash_targets = []

        for field in db_schema:
            f_name = field.name
            if f_name in ["row_id", "created_at"]: continue

            if f_name in actual_cols:
                select_exprs.append(col(f_name).cast(field.dataType))
            elif f_name == "emp_id" and "employee_id" in actual_cols:
                select_exprs.append(col("employee_id").cast(field.dataType).alias("emp_id"))
            else:
                select_exprs.append(lit(None).cast(field.dataType).alias(f_name))
            
            # 지문 생성용 리스트 (변동 컬럼 제외한 나머지 전체)
            if f_name not in exclude_from_hash:
                hash_targets.append(lower(trim(coalesce(col(f_name).cast("string"), lit("null")))))

        # 3. 데이터프레임 구성 및 row_id 생성
        # 실제 데이터는 모든 값을 유지하되, row_id(지문)는 변하지 않는 값들로만 만듭니다.
        df_base = df.select(*select_exprs)
        df_with_id = df_base.withColumn("row_id", md5(concat_ws("|", *hash_targets)))

        # 4. DB와 대조하여 중복 제거
        df_existing = df.sparkSession.read.jdbc(url=jdbc_url, table=f"(SELECT row_id FROM {table_name}) as t", properties=db_props)
        df_final = df_with_id.join(df_existing, "row_id", "left_anti").dropDuplicates(["row_id"])
        
        new_count = df_final.count()
        if new_count > 0:
            logger.info(f"✨ 지문 대조 완료: 새로운 데이터 {new_count}건을 적재합니다.")
            df_final.write.mode("append").jdbc(url=jdbc_url, table=table_name, properties=db_props)
            logger.info("✅ [RDB 적재] 완료.")
        else:
            logger.info("😎 모든 데이터가 내용상 이미 DB에 존재합니다. (적재 스킵)")

    except Exception as e:
        logger.error(f"❌ 적재 프로세스 중 오류 발생: {e}")
        
    return df