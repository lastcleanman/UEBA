import json
import random
import os
from datetime import datetime, timedelta

# [설계서 Ⅱ-1] 로그 저장 경로 설정
LOG_DIR = "/UEBA/data/logs/"
AUTH_LOG = os.path.join(LOG_DIR, "server_auth.log")
CONNECT_LOG = os.path.join(LOG_DIR, "server_connect.log")

def generate_30day_timestamp():
    """오늘로부터 과거 30일 범위 내의 랜덤 타임스탬프 생성"""
    now = datetime.utcnow()
    # 0~29일 전 사이의 무작위 날짜 선택
    random_days = random.randint(0, 29)
    random_hours = random.randint(0, 23)
    random_minutes = random.randint(0, 59)
    dt = now - timedelta(days=random_days, hours=random_hours, minutes=random_minutes)
    
    # [분석 패턴] 주말 트래픽 감소 현상을 인위적으로 생성
    if dt.weekday() >= 5 and random.random() > 0.4:
        dt = dt - timedelta(days=random.randint(1, 3))
        
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

os.makedirs(LOG_DIR, exist_ok=True)

# 1. Server_Auth_Logs 생성 (100건)
with open(AUTH_LOG, 'w', encoding='utf-8') as f:
    users = ["admin", "user01", "dev_manager", "security_audit"]
    ips = ["192.168.1.10", "10.0.0.55", "172.16.0.21", "211.23.1.5"]
    for _ in range(100):
        log = {
            "timestamp": generate_30day_timestamp(),
            "ip": random.choice(ips), "id": random.choice(users),
            "process": "sshd", "message": "Accepted password"
        }
        f.write(json.dumps(log) + "\n")

# 2. Server_Connect_Logs 생성 (100건)
with open(CONNECT_LOG, 'w', encoding='utf-8') as f:
    actions = ["db_access", "file_download", "sudo_exec"]
    for _ in range(100):
        log = {
            "timestamp": generate_30day_timestamp(),
            "user_ip": f"172.16.0.{random.randint(10, 200)}",
            "user_id": random.choice(["manager_kim", "dev_park", "admin"]),
            "action": random.choice(actions)
        }
        f.write(json.dumps(log) + "\n")

print(f">>> [SUCCESS] 30일치 테스트 데이터 400건 생성 완료")