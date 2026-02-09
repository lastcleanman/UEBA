import hashlib
from elasticsearch import Elasticsearch, helpers

class ESLoader:
    def __init__(self, host, port):
        if not host.startswith("http"):
            host = f"http://{host}"
        self.client = Elasticsearch(f"{host}:{port}")

    def load(self, df, index_name, id_cols=None):
        """
        :param df: 적재할 Pandas DataFrame
        :param index_name: ES 인덱스 이름
        :param id_cols: List[str] - Unique ID를 생성할 기준 컬럼 리스트
        """
        if df.empty:
            return 0

        # DataFrame -> Dict List 변환
        records = df.to_dict(orient='records')
        actions = []
        
        for row in records:
            action = {
                "_index": index_name,
                "_source": row
            }
            
            # ▼▼▼ [중복 방지 로직] ▼▼▼
            # id_cols(예: ['employee_id'])가 있으면 고유 ID 생성
            if id_cols:
                try:
                    # 1. 기준 컬럼들의 값을 문자열로 연결 (예: "1001")
                    # 여러 컬럼인 경우 "값1-값2" 형태로 연결됨
                    unique_str = "-".join([str(row.get(col, '')) for col in id_cols])
                    
                    # 2. MD5 해시 생성 (길이 일정, 특수문자 문제 해결)
                    # 결과 예: "1001" -> "b8c37e..." (항상 동일한 값 나옴)
                    doc_id = hashlib.md5(unique_str.encode('utf-8')).hexdigest()
                    
                    # 3. _id 필드에 지정 (이 ID가 같으면 덮어쓰기 됨)
                    action["_id"] = doc_id
                    action["_op_type"] = "index"  # index 모드: 있으면 Update, 없으면 Create
                    
                except Exception as e:
                    print(f"⚠️ ID 생성 실패 (Row 건너뜀): {e}")
                    continue

            actions.append(action)

        # Bulk Insert 실행
        try:
            success, _ = helpers.bulk(self.client, actions)
            return success
        except Exception as e:
            print(f"!!! ES 적재 에러: {e}")
            raise e