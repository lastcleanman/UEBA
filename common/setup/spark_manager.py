from pyspark.sql import SparkSession
from common.setup.config import SPARK_APP_NAME, SPARK_MASTER

def get_spark_session():
    """
    JDBC 드라이버를 포함한 Spark 세션을 생성합니다.
    """
    spark = SparkSession.builder \
        .appName(SPARK_APP_NAME) \
        .master(SPARK_MASTER) \
        .config("spark.jars", "/UEBA/library/mysql-connector-j-8.0.33.jar") \
        .config("spark.driver.extraClassPath", "/UEBA/library/mysql-connector-j-8.0.33.jar") \
        .config("spark.executor.extraClassPath", "/UEBA/library/mysql-connector-j-8.0.33.jar") \
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
        .getOrCreate()
        
    # 로그 레벨 조정
    spark.sparkContext.setLogLevel("WARN")
    
    return spark
