from elasticsearch import Elasticsearch, helpers
import time

class ESLoader:
    def __init__(self, host, port):
        self.es_url = f"http://{host}:{port}"
        self.client = Elasticsearch(self.es_url)

    def load(self, pandas_df, index_name, id_cols=None):
        """Pandas DataFrame을 ES에 Bulk Insert"""
        if pandas_df.empty:
            return 0

        # DataFrame을 딕셔너리 리스트로 변환 (ES 입력용)
        records = pandas_df.to_dict(orient='records')

        actions = []
        for row in records:
            action = {
                "_index": index_name,
                "_source": row
            }
            
            # ID 컬럼이 지정되어 있으면 _id 설정 (중복 방지)
            if id_cols:
                # 여러 컬럼을 조합해서 고유 ID 생성 (예: 날짜_사용자ID)
                doc_id = "_".join([str(row.get(col, '')) for col in id_cols])
                action["_id"] = doc_id
            
            actions.append(action)

        try:
            # 대량 삽입 실행
            success, failed = helpers.bulk(self.client, actions, stats_only=True)
            return success
        except Exception as e:
            print(f"!!! [ES Error] 적재 실패: {e}")
            return 0