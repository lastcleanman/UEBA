from pyspark.sql.functions import col, lit, date_format, coalesce, current_timestamp
from common.setup.logger import get_logger

logger = get_logger("Processing")

def normalize_data(spark, raw_pandas_df, source_name):
    try:
        raw_pandas_df = raw_pandas_df.astype(str).replace({'nan': '', 'None': '', 'NaT': ''})
    except: pass

    df = raw_pandas_df
    
    required_cols = ["user_id", "employee_id", "name", "name_kr", "action", "log_source", "src_ip", "department"]
    for c in required_cols:
        if c not in df.columns: df = df.withColumn(c, lit(None).cast("string"))
    if "salary" not in df.columns: df = df.withColumn("salary", lit(0.0))

    df = df.withColumn("user_id", coalesce(col("user_id"), col("employee_id"), col("name"), col("name_kr"), lit("Unknown_User")))
    df = df.fillna({
        "action": "access_log", "log_source": source_name,
        "src_ip": "Internal", "department": "Unknown_Dept"
    })
    df = df.withColumn("src_ip", coalesce(col("src_ip"), lit("Internal")))
    df = df.withColumn("department", coalesce(col("department"), lit("Unknown_Dept")))

    time_cols = []
    if "updated_at" in df.columns: time_cols.append(col("updated_at"))
    if "hire_date" in df.columns: time_cols.append(col("hire_date"))
    if "timestamp" in df.columns: time_cols.append(col("timestamp"))
    time_cols.append(current_timestamp())

    df = df.withColumn("final_ts", coalesce(*time_cols))
    df = df.withColumn("timestamp", date_format(col("final_ts"), "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("timestamp", coalesce(col("timestamp"), date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")))

    logger.info(f"[{source_name}] 데이터 정제 완료")
    return df