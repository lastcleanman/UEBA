import os
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Real_Data_Ingestion_Final") \
    .config("spark.es.nodes", "ueba-elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

try:
    print(">>> [1] MariaDB 새 테이블(sj_ueba_emp) 읽기...")
    
    # ★ 수정된 부분: 테이블 이름을 'sj_ueba_emp'로 변경했습니다.
    maria_df = spark.read.format("jdbc") \
        .option("url", "jdbc:mariadb://192.168.0.131:13306/UEBA_TEST?zeroDateTimeBehavior=convertToNull") \
        .option("dbtable", "sj_ueba_emp") \
        .option("user", "ueba_user") \
        .option("password", "Suju!0901") \
        .option("driver", "org.mariadb.jdbc.Driver") \
        .load()

    print(">>> 데이터 확인 (김철수, 이영희가 보여야 성공):")
    maria_df.show(5, truncate=False)

    print(">>> [2] Elasticsearch 전송...")
    # 기존 인덱스와 꼬이지 않게 새 인덱스 이름(ueba_emp_new)을 사용하는 것을 추천합니다.
    maria_df.write.format("es") \
        .option("es.mapping.id", "employee_id") \
        .mode("overwrite") \
        .save("ueba_emp_logs")
    
    print(">>> 성공! 10분 대기합니다.")
    time.sleep(600)

except Exception as e:
    print(f"!!! 에러: {e}")

finally:
    spark.stop()