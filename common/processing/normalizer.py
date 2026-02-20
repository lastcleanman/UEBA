import xml.etree.ElementTree as ET
import os
from pyspark.sql.functions import col, lit, date_format, coalesce, current_timestamp
from common.setup.logger import get_logger

logger = get_logger("Processing")

def load_parser_rules(source_name, base_dir="/UEBA/common/parser/"):
    """장비명과 동일한 이름의 분리된 XML 파일을 동적으로 읽어옵니다."""
    xml_path = os.path.join(base_dir, f"{source_name}.xml")
    mappings = {}
    
    if not os.path.exists(xml_path):
        logger.warning(f"⚠️ XML 파서 설정 파일이 없습니다: {xml_path}")
        return mappings
    
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        for field in root.iter('field'):
            target = field.get('target')
            source = field.get('source')
            if target and source:
                mappings[target] = source
    except Exception as e:
        logger.error(f"❌ [{source_name}] XML 파싱 에러: {e}")
    return mappings

def normalize_data(spark, raw_pandas_df, source_name):
    try:
        raw_pandas_df = raw_pandas_df.astype(str).replace({'nan': '', 'None': '', 'NaT': ''})
    except: pass

    df = raw_pandas_df
    
    # 💡 1. XML에서 동적 매핑 룰 가져오기
    mappings = load_parser_rules(source_name)
    mapped_count = 0
    
    # 💡 2. XML 룰에 따라 컬럼 매핑 (원본에 source 필드가 있으면 target 필드로 복사)
    for target_col, source_col in mappings.items():
        if source_col in df.columns:
            df = df.withColumn(target_col, col(source_col))
            mapped_count += 1
            
    if mappings:
        logger.info(f"[{source_name}] 📜 XML 파서 적용 완료: {mapped_count}개 필드 매핑됨")

    # 💡 3. 엔진이 요구하는 UEBA 표준 필수 필드 껍데기 보장
    required_cols = ["user_id", "user", "department", "action", "src_ip", "log_source"]
    for c in required_cols:
        if c not in df.columns: 
            df = df.withColumn(c, lit(None).cast("string"))

    # 💡 4. 결측치(Null)에 대한 최후의 기본값 설정 (에러 방지용)
    df = df.withColumn("user_id", coalesce(col("user_id"), lit("Unknown_ID")))
    df = df.withColumn("user", coalesce(col("user"), lit("Unknown_User")))
    df = df.withColumn("department", coalesce(col("department"), lit("Unknown_Dept")))
    df = df.withColumn("src_ip", coalesce(col("src_ip"), lit("Internal")))
    df = df.withColumn("action", coalesce(col("action"), lit("Unknown_Action")))
    df = df.withColumn("log_source", lit(source_name))

    # 💡 5. 시간 필드 처리
    time_cols = []
    if "final_ts" in df.columns: time_cols.append(col("final_ts"))
    if "timestamp" in df.columns: time_cols.append(col("timestamp"))
    time_cols.append(current_timestamp())

    df = df.withColumn("final_ts", coalesce(*time_cols))
    df = df.withColumn("timestamp", date_format(col("final_ts"), "yyyy-MM-dd HH:mm:ss"))

    logger.info(f"[{source_name}] ✨ 데이터 정제 완료")
    return df