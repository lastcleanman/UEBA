import sys
import os
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from datetime import datetime
import re 

# ES 설정
ES_HOST = "http://ueba-elasticsearch:9200"

def fetch_logs_from_es(es):
    """1. Elasticsearch에서 학습할 로그 데이터 가져오기"""
    print(">>> [1] ES에서 데이터 로딩 중 (ueba-auth-logs)...")
    
    # 최근 데이터 1000건만 가져오기
    query = {
        "size": 1000,
        "query": {
            "match_all": {}
        }
    }
    
    resp = es.search(index="ueba-auth-logs", body=query)
    hits = resp['hits']['hits']
    
    data = []
    for h in hits:
        source = h['_source']
        source['_id'] = h['_id'] 
        data.append(source)
        
    df = pd.DataFrame(data)
    print(f"   - 로드된 데이터: {len(df)} 건")
    return df

def preprocess_features(df):
    """2. 데이터 전처리 및 피처 엔지니어링"""
    print(">>> [2] 데이터 전처리 및 피처 엔지니어링...")
    
    if df.empty:
        return pd.DataFrame()

    features = df.copy()

    # [수정] IP 주소 추출 로직 (정규표현식)
    def extract_ip(message):
        # "from 192.168.0.99" 패턴 찾기
        match = re.search(r'from\s+(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', str(message))
        return match.group(1) if match else "Unknown"

    # 원본 df에 'src_ip' 컬럼 추가 (나중에 결과 저장용)
    df['src_ip'] = df['message'].apply(extract_ip)

    # A. 시간 정보 추출
    features['hour'] = features['timestamp'].str.extract(r'(\d{2}):\d{2}:\d{2}').astype(int)

    # B. 행위(Process)를 숫자로 변환
    features['is_sshd'] = features['process'].apply(lambda x: 1 if 'sshd' in str(x) else 0)
    features['is_sudo'] = features['process'].apply(lambda x: 1 if 'sudo' in str(x) else 0)

    # C. 'Failed' 키워드 가중치
    features['fail_count'] = features['message'].apply(lambda x: 1 if 'Failed' in str(x) else 0)

    # AI 모델용 피처 선택
    return features[['hour', 'is_sshd', 'is_sudo', 'fail_count']]

def train_and_predict(df, model_input_df):
    """3. Isolation Forest 모델 학습 및 이상 탐지"""
    print(">>> [3] AI 모델 학습 (Isolation Forest)...")
    
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(model_input_df)

    # contamination=0.3 (전체의 30%를 이상으로 간주 - 민감도 높임)
    model = IsolationForest(n_estimators=100, contamination=0.3, random_state=42)
    
    model.fit(X_scaled)
    df['anomaly_label'] = model.predict(X_scaled) 
    df['anomaly_score'] = model.decision_function(X_scaled)
    
    df['prediction'] = df['anomaly_label'].apply(lambda x: 'ANOMALY' if x == -1 else 'NORMAL')
    
    anomaly_count = df[df['prediction'] == 'ANOMALY'].shape[0]
    print(f"   - 탐지된 이상 징후: {anomaly_count} 건")
    
    return df

def save_results_to_es(es, df):
    """4. 분석 결과 저장"""
    print(">>> [4] 분석 결과 저장 중...")
    
    actions = []
    for _, row in df.iterrows():
        if row['prediction'] == 'ANOMALY':
            # 백서 기반 리스크 스코어링 (간이 버전)
            # 점수가 낮을수록(-0.2) 더 위험하므로 절대값에 비례하게 0~100점 변환
            risk_score = min(100, int(abs(row['anomaly_score']) * 400))
            
            action = {
                "_index": "ueba-alerts",
                "_source": {
                    "original_log_id": row['_id'],
                    "timestamp": datetime.now().isoformat(),
                    "detected_type": "Brute Force Attempt", 
                    "severity": "CRITICAL" if risk_score > 80 else "HIGH",
                    "details": row['message'],
                    "src_ip": row.get('src_ip', 'Unknown'), # 추출된 IP 저장
                    "risk_score": risk_score,
                    "anomaly_score": float(row['anomaly_score'])
                }
            }
            actions.append(action)

    if actions:
        success, _ = helpers.bulk(es, actions)
        print(f"   - [SUCCESS] {success} 건의 경보(Alert)가 생성되었습니다!")
    else:
        print("   - 저장할 이상 징후가 없습니다.")

def main():
    try:
        es = Elasticsearch(ES_HOST)
        if not es.ping():
            print(f"!!! ES 접속 실패: {ES_HOST}")
            return

        # 1. 데이터 로드
        raw_df = fetch_logs_from_es(es)
        if raw_df.empty:
            print("데이터가 없어 종료합니다.")
            return

        # 2. 전처리
        model_input = preprocess_features(raw_df)

        # 3. 모델링
        result_df = train_and_predict(raw_df, model_input)

        # 4. 결과 저장
        save_results_to_es(es, result_df)
        
        print("\n>>> 분석 완료. Kibana에서 'ueba-alerts' 인덱스를 확인하세요.")

    except Exception as e:
        print(f"!!! 에러 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()