import sys
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from datetime import datetime, timedelta
import re

# ES 설정
ES_HOST = "http://ueba-elasticsearch:9200"

def fetch_recent_logs(es):
    """최근 로그 가져오기"""
    print(">>> [1] 시나리오 분석을 위한 데이터 로드...")
    
    # [수정] sort 부분을 제거했습니다. (텍스트 필드 정렬 에러 방지)
    # 대신 size를 넉넉하게 잡아서 데이터를 가져온 후 Pandas에서 정렬합니다.
    query = {
        "size": 5000, 
        "query": {
            "match": {
                "process": "sshd" # sshd 로그만 분석
            }
        }
    }
    resp = es.search(index="ueba-auth-logs", body=query)
    
    data = []
    # [수정] 문법 오류 방지를 위해 := 연산자 대신 일반 할당 사용
    hits = resp['hits']['hits']
    for h in hits:
        src = h['_source']
        src['_id'] = h['_id']
        data.append(src)
        
    return pd.DataFrame(data)

def detect_bruteforce_success(df):
    """
    [탐지 로직]
    동일 IP에서 'Failed'가 3회 이상 발생한 직후, 
    1분 이내에 'Accepted'가 발생하면 '계정 탈취'로 판단
    """
    print(">>> [2] 시나리오 패턴 매칭 중 (Fail -> Success)...")
    
    if df.empty: return []

    # IP 추출 함수
    def get_ip(msg):
        m = re.search(r'from\s+(\d+\.\d+\.\d+\.\d+)', str(msg))
        return m.group(1) if m else None
    
    # 데이터 전처리
    df['src_ip'] = df['message'].apply(get_ip)
    
    # [중요] 날짜 파싱 (Elasticsearch 대신 여기서 정렬하기 위해 변환 필수)
    # 로그 포맷: "Feb 10 15:37:01"
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%b %d %H:%M:%S', errors='coerce')
    
    # 결과 저장 리스트
    detected_alerts = []

    # IP별로 그룹화하여 분석
    for ip, group in df.groupby('src_ip'):
        if ip is None: continue
        
        # [핵심] 여기서 시간순으로 정렬합니다 (ES가 못한 일을 대신 수행)
        group = group.sort_values('timestamp')
        
        # 상태 추적 변수
        fail_count = 0
        last_fail_time = None
        
        for _, row in group.iterrows():
            msg = str(row['message']) # 문자열 변환 안전장치
            
            if "Failed" in msg:
                fail_count += 1
                last_fail_time = row['timestamp']
            
            elif "Accepted" in msg:
                # 조건: 실패가 3회 이상 쌓여있고, 마지막 실패 후 60초 이내에 성공했다면?
                if fail_count >= 3 and last_fail_time is not pd.NaT:
                    # 시간 차이 계산 (초 단위)
                    time_diff = (row['timestamp'] - last_fail_time).total_seconds()
                    
                    # 0초 ~ 60초 이내 성공 시 탐지
                    if 0 <= time_diff <= 60:
                        print(f"   !!! [CRITICAL] {ip} 계정 탈취 탐지! ({fail_count}회 실패 후 성공)")
                        
                        alert = {
                            "_index": "ueba-alerts",
                            "_source": {
                                "original_log_id": row['_id'],
                                "timestamp": datetime.utcnow().isoformat(), # UTC 시간 사용
                                "detected_type": "Scenario: Brute Force Success",
                                "severity": "CRITICAL", # 최고 등급
                                "risk_score": 100,      # 위험도 만점
                                "src_ip": ip,
                                "details": f"Detected {fail_count} failures followed by success within {int(time_diff)}s",
                                "anomaly_score": -1.0
                            }
                        }
                        detected_alerts.append(alert)
                
                # 성공했으니 카운트 초기화 (세션 리셋)
                fail_count = 0
                last_fail_time = None
                
    return detected_alerts

def main():
    try:
        es = Elasticsearch(ES_HOST)
        if not es.ping():
            print(f"!!! ES 접속 실패: {ES_HOST}")
            return

        # 1. 데이터 로드
        df = fetch_recent_logs(es)
        
        # 2. 시나리오 분석
        alerts = detect_bruteforce_success(df)
        
        # 3. 경보 저장
        if alerts:
            success, _ = helpers.bulk(es, alerts)
            print(f">>> [3] {success} 건의 '계정 탈취' 경보 발령 완료!")
        else:
            print(">>> [3] 특이 시나리오 미탐지.")

    except Exception as e:
        print(f"!!! 에러 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()