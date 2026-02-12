import os
import json
import pandas as pd
from datetime import datetime, timedelta
import random
from sqlalchemy import create_engine

LOG_DIR = "/UEBA/data/logs"
os.makedirs(LOG_DIR, exist_ok=True)

# =====================================================================
# 💡 [로그 포맷 템플릿 설정]
# 나중에 포맷을 바꾸고 싶다면 아래 문자열의 모양만 마음대로 수정하시면 됩니다!
# 중괄호 {} 안의 값은 코드가 실행될 때 자동으로 실제 데이터로 치환됩니다.
# =====================================================================
FW_LOG_TEMPLATE = (
    "[{action}] [{src_ip}] start_time=\"{timestamp}\" end_time=\"{end_time}\" duration=\"{duration}\" "
    "machine_name={machine_name} fw_rule_id={fw_rule_id} src_ip={src_ip} user_id={user_id} "
    "src_port={src_port} dst_ip={dst_ip} dst_port={dst_port} protocol={protocol} "
    "app_name={app_name} packets_total={packets} bytes_total={bytes}"
)


def fetch_real_users():
    """MariaDB의 sj_ueba_hr 테이블에서 실제 사번, 부서명, 할당 IP를 가져옵니다."""
    try:
        config_path = "/UEBA/common/setup/db_sources.json"
        with open(config_path, "r", encoding="utf-8") as f:
            sources = json.load(f)
        
        maria_conf = next((s for s in sources if s["name"] == "ueba_mariaDB"), None)
        if not maria_conf:
            raise ValueError("MariaDB 설정(ueba_mariaDB)을 찾을 수 없습니다.")

        url = f"mysql+pymysql://{maria_conf['user']}:{maria_conf['password']}@{maria_conf['host']}:{maria_conf['port']}/{maria_conf['db_name']}"
        engine = create_engine(url)
        
        query = "SELECT emp_id AS user_id, dept_name AS department, static_ip AS src_ip FROM sj_ueba_hr WHERE emp_id IS NOT NULL"
        df = pd.read_sql(query, engine)

        valid_users = []
        for _, row in df.iterrows():
            uid = row['user_id']
            dept = row['department'] if pd.notna(row['department']) else 'Unknown_Dept'
            ip = row['src_ip'] if pd.notna(row['src_ip']) and str(row['src_ip']).strip() != "" else f"192.168.1.{random.randint(2, 254)}"
            
            valid_users.append({"user_id": str(uid), "department": str(dept), "src_ip": str(ip)})
            
        print(f"✅ MariaDB 연동 성공: 총 {len(valid_users)}명의 실제 직원 정보를 불러왔습니다.")
        return valid_users
    except Exception as e:
        print(f"❌ DB 연동 실패: {e} (기본 가상 데이터로 진행합니다.)")
        return [{"user_id": f"user{i:03d}", "department": "Sales", "src_ip": f"192.168.1.{i+10}"} for i in range(1, 11)]


def generate_custom_format_logs(valid_users):
    """요청하신 Key-Value 포맷 템플릿을 사용하여 방화벽 스타일 로그를 대량 생성합니다."""
    log_lines = []
    now = datetime.now()
    
    # 1. [정상] 모든 직원이 무작위로 1~10번씩 방화벽 로그를 발생시킴 (대용량)
    for user in valid_users:
        # 각 직원당 1건 ~ 10건의 정상 로그를 무작위로 생성
        for _ in range(random.randint(1, 10)):
            ts = now - timedelta(hours=random.randint(1, 72)) # 최근 3일 치 데이터
            duration = random.randint(1, 120)
            
            log_data = {
                "action": random.choice(["fw4_allow", "fw4_allow", "fw6_allow"]),
                "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": (ts + timedelta(seconds=duration)).strftime("%Y-%m-%d %H:%M:%S"),
                "duration": duration,
                "machine_name": "FW-Core-01",
                "fw_rule_id": f"Rule_{random.randint(10, 50)}",
                "src_ip": user["src_ip"],
                "user_id": user["user_id"],
                "src_port": random.randint(10000, 60000),
                "dst_ip": f"10.10.10.{random.randint(1, 50)}",
                "dst_port": random.choice([80, 443, 8080]),
                "protocol": "TCP",
                "app_name": random.choice(["Web-Browsing", "Office365", "Slack"]),
                "packets": random.randint(10, 500),
                "bytes": random.randint(1024, 50000)
            }
            log_lines.append(FW_LOG_TEMPLATE.format(**log_data))

    # =================================================================
    # 🚨 [위협 강화] SOC 시연용 4대 치명적 해킹 시나리오 강제 주입
    # =================================================================
    target_users = random.sample(valid_users, min(5, len(valid_users))) # 타겟 5명 선정
    weekend_time = now - timedelta(days=now.weekday() + 1)
    
    for i, target_user in enumerate(target_users):
        night_time = weekend_time.replace(hour=random.randint(1, 4), minute=random.randint(0, 59), second=0)
        
        # 1. 랜섬웨어/C2 서버 비인가 통신 (Reverse Shell)
        if i == 0:
            for j in range(3):
                ts = night_time + timedelta(minutes=j*2)
                log_data = {"action": "fw6_drop", "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"), "end_time": (ts + timedelta(seconds=5)).strftime("%Y-%m-%d %H:%M:%S"), "duration": 5, "machine_name": "FW-Core-01", "fw_rule_id": "Rule_Block_C2", "src_ip": target_user["src_ip"], "user_id": target_user["user_id"], "src_port": 4444, "dst_ip": "185.10.10.2", "dst_port": 4444, "protocol": "TCP", "app_name": "Reverse_Shell_C2", "packets": 500, "bytes": 15000}
                log_lines.append(FW_LOG_TEMPLATE.format(**log_data))

        # 2. 내부자 대규모 기밀 유출 (FTP/클라우드 대용량 전송)
        elif i == 1:
            ts = night_time + timedelta(minutes=15)
            log_data = {"action": "fw4_allow", "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"), "end_time": (ts + timedelta(seconds=1800)).strftime("%Y-%m-%d %H:%M:%S"), "duration": 1800, "machine_name": "FW-Core-01", "fw_rule_id": "Rule_Bypass", "src_ip": target_user["src_ip"], "user_id": target_user["user_id"], "src_port": 55112, "dst_ip": "104.20.15.10", "dst_port": 21, "protocol": "TCP", "app_name": "Massive_FTP_Exfiltration", "packets": 999999, "bytes": 8500000000}
            log_lines.append(FW_LOG_TEMPLATE.format(**log_data))

        # 3. 무차별 대입 공격 (SSH Brute Force)
        elif i == 2:
            for j in range(50): # 50번 연속 실패 (로그 도배)
                ts = night_time + timedelta(seconds=j*2)
                log_data = {"action": "fw4_drop", "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"), "end_time": (ts + timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S"), "duration": 1, "machine_name": "FW-Core-01", "fw_rule_id": "Rule_SSH", "src_ip": target_user["src_ip"], "user_id": target_user["user_id"], "src_port": random.randint(30000, 60000), "dst_ip": "192.168.10.5", "dst_port": 22, "protocol": "TCP", "app_name": "SSH_BruteForce", "packets": 10, "bytes": 512}
                log_lines.append(FW_LOG_TEMPLATE.format(**log_data))

        # 4. DB 덤프 및 권한 상승 시도
        else:
            ts = night_time + timedelta(minutes=30)
            log_data = {"action": "fw4_allow", "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"), "end_time": (ts + timedelta(seconds=600)).strftime("%Y-%m-%d %H:%M:%S"), "duration": 600, "machine_name": "FW-Core-01", "fw_rule_id": "Rule_DB_Dump", "src_ip": target_user["src_ip"], "user_id": target_user["user_id"], "src_port": random.randint(40000, 50000), "dst_ip": "192.168.100.10", "dst_port": 1521, "protocol": "TCP", "app_name": "Unauthorized_DB_Dump", "packets": 50000, "bytes": 2000000000}
            log_lines.append(FW_LOG_TEMPLATE.format(**log_data))

    # 파일 저장 부분
    file_path = os.path.join(LOG_DIR, "firewall_traffic.log")
    with open(file_path, "w", encoding="utf-8") as f:
        for line in log_lines:
            f.write(line + "\n")
            
    print(f"✅ [생성 완료] 방화벽 트래픽 로그: {file_path} ({len(log_lines):,}건)")


if __name__ == "__main__":
    print("====== 가상 보안 위협 로그 생성을 시작합니다 ======")
    valid_users_list = fetch_real_users()
    
    if valid_users_list:
        # 방화벽 포맷 로그 생성 실행
        generate_custom_format_logs(valid_users_list)
        
    print("====== 가상 로그 생성 완료 ======")