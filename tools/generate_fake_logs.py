import os
import json
import pandas as pd
from datetime import datetime, timedelta
import random
from sqlalchemy import create_engine

LOG_DIR = "/UEBA/data/logs"
os.makedirs(LOG_DIR, exist_ok=True)

# =====================================================================
# 💡 [로그 포맷 템플릿]
# =====================================================================
FW_LOG_TEMPLATE = (
    "[{action}] [{src_ip}] start_time=\"{timestamp}\" end_time=\"{end_time}\" duration=\"{duration}\" "
    "machine_name={machine_name} fw_rule_id={fw_rule_id} src_ip={src_ip} user_id={user_id} "
    "src_port={src_port} dst_ip={dst_ip} dst_port={dst_port} protocol={protocol} "
    "app_name={app_name} packets_total={packets} bytes_total={bytes}"
)

# 대시보드에 예쁘게 나올 주요 부서 목록 (가상 데이터 생성 시 사용)
TARGET_DEPTS = ["플랫폼개발팀", "클라우드보안팀", "인사총무팀", "재무회계팀", "국내영업1팀", "전략기획팀"]

def fetch_real_users():
    """
    MariaDB에서 '부서 정보가 확실한' 사용자만 선별하여 가져옵니다.
    Unknown이나 NULL 부서를 가진 사용자는 로그 생성 대상에서 제외하여 'Other'를 줄입니다.
    """
    try:
        config_path = "/UEBA/common/setup/db_sources.json"
        with open(config_path, "r", encoding="utf-8") as f:
            sources = json.load(f)
        
        maria_conf = next((s for s in sources if s["name"] == "ueba_mariaDB"), None)
        if not maria_conf:
            raise ValueError("MariaDB 설정(ueba_mariaDB)을 찾을 수 없습니다.")

        url = f"mysql+pymysql://{maria_conf['user']}:{maria_conf['password']}@{maria_conf['host']}:{maria_conf['port']}/{maria_conf['db_name']}"
        engine = create_engine(url)
        
        # [핵심 수정] 부서가 없거나 Unknown인 사람은 아예 SQL 단계에서 제외합니다.
        query = """
            SELECT emp_id AS user_id, dept_name AS department, static_ip AS src_ip 
            FROM sj_ueba_hr 
            WHERE emp_id IS NOT NULL 
              AND dept_name IS NOT NULL 
              AND dept_name != 'Unknown_Dept' 
              AND dept_name != ''
        """
        df = pd.read_sql(query, engine)

        if df.empty:
            print("⚠️ 주의: DB에 부서 정보가 있는 사용자가 없습니다. 가상 데이터를 생성합니다.")
            raise Exception("No valid users found")

        valid_users = df.to_dict('records')
            
        print(f"✅ MariaDB 연동 성공: 부서가 확인된 {len(valid_users)}명의 직원을 대상으로 로그를 생성합니다.")
        return valid_users

    except Exception as e:
        print(f"❌ DB 연동 실패 또는 데이터 부족: {e}")
        print("💡 대시보드용 가상 '우수 부서' 데이터를 강제로 생성합니다.")
        
        # DB 연결이 안 될 경우, 'Other'가 뜨지 않도록 우리가 정의한 예쁜 부서명으로 가상 유저를 만듭니다.
        fake_users = []
        for i in range(50):
            fake_users.append({
                "user_id": f"EMP{i:03d}",
                "department": random.choice(TARGET_DEPTS), # 여기서 확실한 부서를 지정
                "src_ip": f"192.168.10.{i+10}"
            })
        return fake_users


def generate_custom_format_logs(valid_users):
    log_lines = []
    now = datetime.now()
    
    # 1. [정상] 데이터 풍부화: 'Other' 비율을 낮추기 위해 정상 부서 로그를 대량(인당 30~50건) 생성
    print("⏳ 정상 업무 로그 대량 생성 중...")
    for user in valid_users:
        # 데이터가 너무 적으면 Other가 커 보일 수 있으므로 생성량을 늘림 (1~10 -> 20~40)
        for _ in range(random.randint(20, 40)): 
            ts = now - timedelta(hours=random.randint(1, 168)) # 최근 7일치
            duration = random.randint(1, 120)
            
            log_data = {
                "action": random.choices(["fw4_allow", "fw4_deny"], weights=[0.9, 0.1])[0],
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
                "app_name": random.choice(["Suju_Groupware", "ERP_System", "Jira", "Slack"]),
                "packets": random.randint(10, 500),
                "bytes": random.randint(1024, 50000)
            }
            log_lines.append(FW_LOG_TEMPLATE.format(**log_data))

    # =================================================================
    # 🚨 [위협] SOC 탐지 시나리오 (타겟 유저도 반드시 부서가 있는 사람으로 선정)
    # =================================================================
    if len(valid_users) >= 5:
        target_users = random.sample(valid_users, 5)
    else:
        target_users = valid_users

    weekend_time = now - timedelta(days=now.weekday() + 1)
    
    for i, target_user in enumerate(target_users):
        night_time = weekend_time.replace(hour=random.randint(1, 4), minute=random.randint(0, 59))
        
        # 1. C2 통신 (Reverse Shell)
        if i == 0:
            for j in range(5): # 탐지 잘 되게 횟수 증가
                ts = night_time + timedelta(minutes=j*2)
                log_data = {"action": "fw6_drop", "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"), "end_time": (ts+timedelta(seconds=5)).strftime("%Y-%m-%d %H:%M:%S"), "duration": 5, "machine_name": "FW-Core-01", "fw_rule_id": "Rule_Block_C2", "src_ip": target_user["src_ip"], "user_id": target_user["user_id"], "src_port": 4444, "dst_ip": "185.10.10.2", "dst_port": 4444, "protocol": "TCP", "app_name": "Reverse_Shell_C2", "packets": 500, "bytes": 15000}
                log_lines.append(FW_LOG_TEMPLATE.format(**log_data))

        # 2. 대량 데이터 유출
        elif i == 1:
            ts = night_time + timedelta(minutes=15)
            log_data = {"action": "fw4_allow", "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"), "end_time": (ts+timedelta(seconds=1800)).strftime("%Y-%m-%d %H:%M:%S"), "duration": 1800, "machine_name": "FW-Core-01", "fw_rule_id": "Rule_Bypass", "src_ip": target_user["src_ip"], "user_id": target_user["user_id"], "src_port": 55112, "dst_ip": "104.20.15.10", "dst_port": 21, "protocol": "TCP", "app_name": "Massive_FTP_Exfiltration", "packets": 999999, "bytes": 8500000000}
            log_lines.append(FW_LOG_TEMPLATE.format(**log_data))

        # 3. SSH Brute Force
        elif i == 2:
            for j in range(50):
                ts = night_time + timedelta(seconds=j*2)
                log_data = {"action": "fw4_drop", "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"), "end_time": (ts+timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S"), "duration": 1, "machine_name": "FW-Core-01", "fw_rule_id": "Rule_SSH", "src_ip": target_user["src_ip"], "user_id": target_user["user_id"], "src_port": random.randint(30000, 60000), "dst_ip": "192.168.10.5", "dst_port": 22, "protocol": "TCP", "app_name": "SSH_BruteForce", "packets": 10, "bytes": 512}
                log_lines.append(FW_LOG_TEMPLATE.format(**log_data))

    # 파일 저장
    file_path = os.path.join(LOG_DIR, "firewall_traffic.log")
    with open(file_path, "w", encoding="utf-8") as f:
        for line in log_lines:
            f.write(line + "\n")
            
    print(f"✅ [생성 완료] 방화벽 트래픽 로그: {file_path} ({len(log_lines):,}건)")
    print(f"ℹ️  이제 'Other' 비율이 줄어들고 {', '.join(TARGET_DEPTS)} 등 주요 부서 위주로 표시될 것입니다.")


if __name__ == "__main__":
    print("====== [개선된] 가상 보안 위협 로그 생성 ======")
    valid_users_list = fetch_real_users()
    
    if valid_users_list:
        generate_custom_format_logs(valid_users_list)
    
    print("====== 생성 완료 ======")