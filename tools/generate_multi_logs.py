import os
import json
import random
import time
from datetime import datetime
from sqlalchemy import create_engine, text

# MariaDB 연결 설정
DB_URL = "mysql+pymysql://ueba_user:Suju!0901@192.168.0.131:13306/UEBA_TEST"
USER_ROSTER = []
LOG_DIR = "/UEBA/data/logs/"

def load_users_from_db():
    print("🔄 MariaDB에서 사원 및 부서 정보를 불러오는 중...")
    engine = create_engine(DB_URL)
    
    try:
        with engine.connect() as conn:
            # ⭐️ e.name_kr (직원 이름) 추가!
            query = text("""
                SELECT 
                    e.employee_id AS emp_id,
                    e.name_kr AS user_name,
                    COALESCE(d.department_name, 'Unknown') AS dept_name
                FROM sj_ueba_employees e
                LEFT JOIN sj_ueba_departments d ON e.department_id = d.department_id
                WHERE e.employee_id IS NOT NULL AND e.name_kr IS NOT NULL
            """)
            result = conn.execute(query)
            
            for idx, row in enumerate(result):
                # IP 및 Device ID 자동 부여
                ip_subnet = (idx % 20) + 10
                ip_host = (idx % 250) + 1
                assigned_ip = f"192.168.{ip_subnet}.{ip_host}"
                
                USER_ROSTER.append({
                    "user_id": row.emp_id,       # 사번 (예: EMP001)
                    "user": row.user_name,       # 이름 (예: 홍길동)
                    "dept": row.dept_name,       # 부서 (예: 인사팀)
                    "ip": assigned_ip,
                    "device_id": f"WS-{row.emp_id}"
                })
                
        print(f"✅ 총 {len(USER_ROSTER)}명의 사원 정보를 성공적으로 로드했습니다!")
        
    except Exception as e:
        print(f"❌ DB 연동 실패: {e}")

def write_log(filename, data):
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR, exist_ok=True)
        
    filepath = os.path.join(LOG_DIR, filename)
    with open(filepath, "a", encoding="utf-8") as f:
        # ⭐️ ensure_ascii=False 를 넣어야 한글 이름이 깨지지 않습니다!
        f.write(json.dumps(data, ensure_ascii=False) + "\n")

def generate_logs(count=5):
    if not USER_ROSTER:
        return
        
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for _ in range(count):
        actor = random.choice(USER_ROSTER)
        
        # 공통으로 들어갈 핵심 사용자 정보
        base_info = {
            "timestamp": now_str,
            "user_id": actor["user_id"], # 사번
            "user": actor["user"],       # 한글 이름
            "department": actor["dept"]  # 부서명
        }
        
        # [1] 인증 로그
        auth_data = {**base_info, "action": random.choices(["login", "logout", "fail"], weights=[70, 20, 10])[0], "ip": actor["ip"]}
        write_log("authentication_activity.log", auth_data)

        # [2] 웹 서버 로그
        web_data = {**base_info, "action": random.choices(["view", "download", "upload"], weights=[80, 15, 5])[0], "resource": random.choice(["/api/v1/data", "/hr/salary.pdf", "/sales/report.xlsx"]), "ip": actor["ip"]}
        write_log("webserver_activity.log", web_data)

        # [3] 엔드포인트 로그
        endpoint_data = {**base_info, "action": random.choices(["process_start", "file_copy", "USB_inserted"], weights=[80, 15, 5])[0], "device_id": actor["device_id"]}
        write_log("endpoint_activity.log", endpoint_data)

        # [4] 방화벽 정책 로그
        fw_data = {**base_info, "src_ip": actor["ip"], "dst_ip": f"10.0.{random.randint(1,5)}.{random.randint(1,255)}", "action": random.choices(["allow", "deny"], weights=[90, 10])[0], "port": random.choice([80, 443, 22])}
        write_log("firewall_activity.log", fw_data)

if __name__ == "__main__":
    print("🚀 고급 JSON UEBA Fake Log 생성기 시작...")
    load_users_from_db()
    try:
        while True:
            generate_logs(5)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n🛑 종료합니다.")