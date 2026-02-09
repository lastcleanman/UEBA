from abc import ABC, abstractmethod

class DBConnector(ABC):
    @abstractmethod
    def connect(self):
        """DB 연결 엔진 생성"""
        pass

    @abstractmethod
    def fetch(self, query: str):
        """데이터 조회 후 DataFrame 반환"""
        pass