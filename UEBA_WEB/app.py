from flask import Flask, render_template
from sqlalchemy import create_engine, text
import os

# templates 폴더 위치를 /UEBA_WEB/templates로 명시적 지정
app = Flask(__name__, template_folder='/UEBA_WEB/templates')

# MariaDB 연결 설정
db_url = "mysql+pymysql://ueba_user:Suju!0901@192.168.0.131:13306/UEBA_TEST"
engine = create_engine(db_url)

@app.route('/')
def index():
    try:
        with engine.connect() as conn:
            # 최신 수집 이력 30건 조회
            query = text("SELECT * FROM ueba_ingestion_history ORDER BY collect_time DESC LIMIT 30")
            result = conn.execute(query)
            history = [dict(row._mapping) for row in result]
        return render_template('index.html', history=history)
    except Exception as e:
        return f"DB 연결 오류 또는 템플릿 파일을 찾을 수 없습니다: {e}"

if __name__ == '__main__':
    # 포트 5000번으로 실행
    app.run(host='0.0.0.0', port=5000)