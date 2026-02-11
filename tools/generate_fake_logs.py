import os
import random
import time
from datetime import datetime

# 저장 경로 (프로젝트 루트의 data/logs 폴더에 저장)
# Docker는 이 폴더를 마운트해서 읽습니다.
LOG_DIR = "data/logs"
LOG_FILE = os.path.join(LOG_DIR, "server_auth.log")

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# 가짜 데이터 소스
USERS = ['root', 'admin', 'developer', 'kdw', 'unknown_user']
IPS = ['192.168.0.10', '192.168.0.25', '10.0.0.5', '203.0.113.42'] 
ACTIONS = [
    "Accepted password for",
    "Failed password for",
    "Disconnected from",
    "sudo: session opened for user root",
    "sudo: session closed for user root"
]

def generate_log_line():
    """Linux Auth Log 형식의 한 줄 생성"""
    # 로그 포맷: Feb 10 15:30:01 hostname process: message
    now = datetime.now().strftime('%b %d %H:%M:%S')
    hostname = "ueba-server-01"
    
    user = random.choice(USERS)
    ip = random.choice(IPS)
    action = random.choice(ACTIONS)
    
    # 시나리오: 'Failed'일 때는 해커일 확률 높임
    if "Failed" in action and random.random() < 0.3:
        user = "hacker"

    # 메시지 조합
    if "sudo" in action:
        message = f"{action} by {user}"
        process = "sudo"
    else:
        message = f"{action} {user} from {ip} port {random.randint(30000, 60000)} ssh2"
        process = f"sshd[{random.randint(1000, 9999)}]"

    return f"{now} {hostname} {process}: {message}\n"

def main():
    print(f">>> [Start] 가짜 로그 생성 시작: {LOG_FILE}")
    print(">>> Ctrl+C를 누르면 중단됩니다.")
    
    try:
        while True:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                # 한 번에 1~5개 로그 생성
                for _ in range(random.randint(1, 5)):
                    line = generate_log_line()
                    f.write(line)
                    print(line.strip()) # 화면에도 출력
            
            # 1~3초 대기 (실시간성 시뮬레이션)
            time.sleep(random.randint(1, 3))
            
    except KeyboardInterrupt:
        print("\n>>> [Stop] 로그 생성 중단")

if __name__ == "__main__":
    main()