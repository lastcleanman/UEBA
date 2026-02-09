import os

# docker-compose.yml의 environment 값을 읽어옵니다.
DB_CONFIG = {
    'type': os.getenv('TARGET_DB_TYPE', 'postgres'),
    'host': os.getenv('TARGET_DB_HOST', 'localhost'),
    # [수정] DB 포트도 정수로 변환하는 것이 안전합니다.
    'port': int(os.getenv('TARGET_DB_PORT', 5432)), 
    'user': os.getenv('TARGET_DB_USER', 'admin'),
    'password': os.getenv('TARGET_DB_PASSWORD', ''),
    'dbname': os.getenv('TARGET_DB_NAME', 'ueba_db'),
    'service_name': os.getenv('TARGET_DB_NAME', 'ORCL')
}

ES_CONFIG = {
    'host': os.getenv('TARGET_ES_HOST', 'localhost'),
    # [수정] 여기가 에러 원인! int()로 감싸주세요.
    'port': int(os.getenv('TARGET_ES_PORT', 9200)) 
}