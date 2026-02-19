import os
import re
import json
import pandas as pd
from common.ingestion.base import BaseConnector
from common.setup.logger import get_logger

logger = get_logger("FileConnector")

class FileConnector(BaseConnector):
    def __init__(self, config):
        super().__init__(config)
        self.file_path = self.config.get("path")
        self.pattern = self.config.get("pattern", "")

    def fetch(self):
        if not os.path.exists(self.file_path):
            logger.error(f"❌ 파일을 찾을 수 없습니다: {self.file_path}")
            return pd.DataFrame()

        # [수정] '만능 파서'는 정규식(pattern)이 없어도 동작해야 하므로 
        # 패턴이 없다고 에러를 뱉고 종료(return)하는 기존 로직은 과감히 삭제했습니다!

        parsed_data = []
    
        with open(self.file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                row_data = {}

                # 💡 [1단계] JSON 형태인지 먼저 확인
                try:
                    row_data = json.loads(line)
                    if 'timestamp' in row_data:
                        row_data['final_ts'] = row_data['timestamp'].replace('T', ' ')
                    parsed_data.append(row_data)
                    continue 
                except json.JSONDecodeError:
                    pass 

                # 💡 [2단계] 기존 정규식 시도
                # 👈 [수정] pattern -> self.pattern 으로 변경
                if self.pattern:
                    match = re.match(self.pattern, line)
                    if match:
                        row_data = match.groupdict()
                        if 'timestamp' in row_data:
                            row_data['final_ts'] = row_data['timestamp']
                        parsed_data.append(row_data)
                        continue

                # 💡 [3단계] 만능 억지 추출 (최후의 수단)
                ts_match = re.search(r'(\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2})', line)
                
                if ts_match:
                    row_data['final_ts'] = ts_match.group(1).replace('T', ' ')
                else:
                    row_data['final_ts'] = "1970-01-01 00:00:00" 
                    
                row_data['raw_message'] = line 
                parsed_data.append(row_data)

        return pd.DataFrame(parsed_data)