import pandas as pd
import json
import os

class FileLogConnector:
    def __init__(self, config):
        """
        설계서 3.5: 엔터프라이즈 급 다중 수집 모드 (File Ingestion)
        """
        self.path = config.get('path')

    def fetch(self):
        """
        JSON 라인 단위로 읽어 데이터프레임으로 변환합니다.
        """
        if not os.path.exists(self.path):
            print(f"   - !!! [ERROR] 파일을 찾을 수 없습니다: {self.path}")
            return pd.DataFrame()

        data = []
        try:
            with open(self.path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"   - [WARN] JSON 파싱 에러 (건너뜀): {e}")
            
            return pd.DataFrame(data)
        except Exception as e:
            print(f"   - !!! [ERROR] 파일 읽기 중 예외 발생: {e}")
            return pd.DataFrame()