from pyspark.sql.functions import col, current_timestamp, when, lit

class SparkDataProcessor:
    def __init__(self, spark):
        self.spark = spark

    def process_for_alerts(self, spark_df, source_name):
        """
        [설계서 3.4] RDB 충돌 원천 제거 및 UML 통합 로직
        """
        if spark_df is None or spark_df.count() == 0:
            return None

        print(f"   ... [Processing] '{source_name}' 시스템 필드 충돌 완벽 차단 중 ...")

        # 1. 표준 필드 매핑 선 수행 (id -> user_id)
        # 원본 id가 ES의 '_id' 메타 필드와 겹치지 않도록 이름을 먼저 바꿉니다.
        mapping_table = {
            "id": "user_id",
            "employee_id": "user_id",
            "ip": "src_ip",
            "user_ip": "src_ip",
            "source_addr": "src_ip"
        }
        
        for raw, std in mapping_table.items():
            if raw in spark_df.columns:
                if std not in spark_df.columns:
                    spark_df = spark_df.withColumnRenamed(raw, std)
                else:
                    # 중복 필드인 경우 원본 제거 (ES 충돌 방지 핵심)
                    spark_df = spark_df.drop(raw)

        # 2. 모든 필드 문자열 캐스팅 (매핑 에러 방지)
        for field in spark_df.schema.fields:
            spark_df = spark_df.withColumn(field.name, col(field.name).cast("string"))

        # 3. 출처 태깅 및 유효성 검증
        spark_df = spark_df.withColumn("log_source", lit(source_name))
        
        # 유효한 IP 형식만 통과 (Garbage In 차단)
        if "src_ip" in spark_df.columns:
            spark_df = spark_df.filter(col("src_ip").rlike(r"(\d{1,3}\.){3}\d{1,3}"))

        # 4. 보안 마스킹 및 타임스탬프 주입
        if "user_id" in spark_df.columns:
            spark_df = spark_df.withColumn("user_id_masked", 
                                          when(col("user_id").isNotNull(), "****")
                                          .otherwise("unknown"))

        return spark_df.withColumn("event_time", current_timestamp())