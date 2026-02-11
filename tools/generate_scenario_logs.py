import os
import random
import time
from datetime import datetime

LOG_DIR = "data/logs"
LOG_FILE = os.path.join(LOG_DIR, "server_auth.log")

def generate_scenario():
    """시나리오: 브루트포스 공격 후 로그인 성공 (가장 위험한 패턴)"""
    
    # 현재 시간 (초 단위 미세 조정 필요)
    now_ts = time.time()
    hostname = "ueba-server-01"
    attacker_ip = "203.0.113.88" # 새로운 공격자 IP
    target_user = "admin"
    
    logs = []
    
    print(f">>> [SCENARIO] '{attacker_ip}'가 '{target_user}' 계정 탈취를 시도합니다...")

    # 1. 5회 로그인 실패 (공격 시도)
    for i in range(5):
        t = datetime.fromtimestamp(now_ts + i).strftime('%b %d %H:%M:%S')
        pid = random.randint(10000, 20000)
        line = f"{t} {hostname} sshd[{pid}]: Failed password for {target_user} from {attacker_ip} port {50000+i} ssh2\n"
        logs.append(line)
        print(f"   - [Fail] 공격 시도 {i+1}")

    # 2. 1회 로그인 성공 (뚫림!)
    t = datetime.fromtimestamp(now_ts + 6).strftime('%b %d %H:%M:%S')
    pid = random.randint(20001, 30000)
    line = f"{t} {hostname} sshd[{pid}]: Accepted password for {target_user} from {attacker_ip} port 50006 ssh2\n"
    logs.append(line)
    print(f"   - [SUCCESS] !!! 계정 탈취 성공 !!!")

    return logs

def main():
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            scenario_logs = generate_scenario()
            f.writelines(scenario_logs)
        print(">>> 로그 주입 완료.")
            
    except Exception as e:
        print(f"!!! 에러 발생: {e}")

if __name__ == "__main__":
    main()