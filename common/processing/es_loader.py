from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from common.setup.logger import get_logger

logger = get_logger("ESLoader")

class ESLoader:
    def __init__(self, host='localhost', port=9200):
        # [수정] 호환성(Compatibility) 모드를 명시적으로 활성화합니다.
        self.es = Elasticsearch(
            f"http://{host}:{port}",
            request_timeout=60,
            max_retries=3,
            retry_on_timeout=True,
            # 💡 버전 9 클라이언트가 버전 8 서버와 통신할 때 발생하는 헤더 에러 방지
            meta_header=False
        )

    def load(self, df, index_name, id_cols=None):
        if df.empty:
            logger.warning("적재할 데이터가 비어 있습니다.")
            return 0

        records = df.to_dict(orient='records')
        actions = []

        for row in records:
            action = {
                "_index": index_name,
                "_source": row
            }
            if id_cols:
                doc_id = "_".join([str(row.get(c, "")) for c in id_cols])
                action["_id"] = doc_id
            actions.append(action)

        try:
            # stats_only=False로 설정하여 상세 에러 정보를 받아옵니다.
            success, items = bulk(self.es, actions, stats_only=False, raise_on_error=False)
            
            failed_items = [item for item in items if item.get('index', {}).get('error')]
            
            if failed_items:
                logger.error(f"❌ 총 {len(failed_items)}건의 데이터 적재 실패!")
                # 너무 많을 수 있으므로 상위 3건만 상세 원인을 출력합니다.
                for i, item in enumerate(failed_items[:3]):
                    error_info = item['index']['error']
                    doc_id = item['index'].get('_id', 'N/A')
                    logger.error(f"  - 실패 원인 [{i+1}]: {error_info['reason']}")
                    logger.error(f"  - 문제 발생 필드: {error_info.get('caused_by', {}).get('reason', 'N/A')}")
                    logger.error(f"  - 문서 ID: {doc_id}")
            
            return success
        except Exception as e:
            logger.error(f"❌ ES Bulk Insert 과정에서 치명적 오류 발생: {e}")
            return 0