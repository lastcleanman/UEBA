import os
import re
import pandas as pd
from common.ingestion.base import BaseConnector
from common.setup.logger import get_logger

logger = get_logger("FileConnector")

class FileConnector(BaseConnector):
    def __init__(self, config):
        super().__init__(config)
        self.file_path = self.config.get("path")
        # [수정] 패턴이 없을 경우를 대비해 기본 빈 문자열 설정
        self.pattern = self.config.get("pattern", "")

    def fetch(self):
        if not os.path.exists(self.file_path):
            logger.error(f"❌ 파일을 찾을 수 없습니다: {self.file_path}")
            return pd.DataFrame()

        # [추가] 패턴 유효성 검사
        if not self.pattern:
            logger.error(f"❌ 정규식 패턴(pattern)이 설정되지 않았습니다. ({self.file_path})")
            return pd.DataFrame()

        parsed_data = []
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                for i, line in enumerate(lines):
                    line = line.strip()
                    if not line: continue
                    
                    match = re.search(str(self.pattern), line)
                    if match:
                        parsed_data.append(match.groupdict())
                    else:
                        # 💡 [디버깅용] 최초 1건의 매칭 실패 사례를 출력합니다.
                        if i == 0:
                            logger.error(f"❌ 정규식 매칭 실패 샘플 (1라인): {line}")
                            logger.error(f"❌ 설정된 패턴: {self.pattern}")
            
            df = pd.DataFrame(parsed_data)
            return df
        except Exception as e:
            # ❌ 여기서 'first argument must be string' 에러가 잡힙니다.
            logger.error(f"❌ 파일 파싱 중 오류 발생: {str(e)}")
            return pd.DataFrame()