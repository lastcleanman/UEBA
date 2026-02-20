import os, json, glob
import xml.etree.ElementTree as ET
from xml.dom import minidom
from sqlalchemy import create_engine, text
from datetime import datetime

DB_URL = "mysql+pymysql://ueba_user:Suju!0901@192.168.0.131:13306/UEBA_TEST"
LOG_DIR = "/UEBA/data/logs/"

def build_parser():
    engine = create_engine(DB_URL)
    log_files = glob.glob(os.path.join(LOG_DIR, "*.log"))
    
    for file_path in log_files:
        filename = os.path.basename(file_path)
        
        # [Step 1] 로그 파일 읽기 및 장비명(source_name) 식별
        source_name = "Unknown"
        if "authentication" in filename: source_name = "Auth_Logs"
        elif "webserver" in filename: source_name = "Web_Logs"
        elif "endpoint" in filename: source_name = "Endpoint_Logs"
        elif "firewall" in filename: source_name = "Firewall_Logs"
        
        if source_name == "Unknown": continue

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                if not first_line: continue
                sample_data = json.loads(first_line)
        except Exception as e:
            continue

        # [Step 2] 샘플 데이터 분석 후 XML 파서 동적 생성
        root = ET.Element("parser", name=source_name)
        
        # (여기에 머신러닝 모델의 추론 결과가 들어간다고 가정)
        inference_rules = {"user": "user_id", "ip": "src_ip", "department": "department", "action": "action"}
        
        has_user = False
        for key in sample_data.keys():
            if key in inference_rules:
                target = inference_rules[key]
                ET.SubElement(root, "field", target=target, source=key)
                if target == "user_id": has_user = True
                
        if has_user:
            ET.SubElement(root, "field", target="emp_name", source="mapped_name")

        xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="    ")

        # [Step 3] 생성된 XML 파서 정보를 DB에 저장
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO sj_ueba_parsers (source_name, parser_xml)
                VALUES (:source, :xml)
                ON DUPLICATE KEY UPDATE parser_xml = :xml, updated_at = CURRENT_TIMESTAMP
            """), {"source": source_name, "xml": xml_str})
            print(f"✅ [{source_name}] 파서 규칙 DB 저장 완료!")

if __name__ == "__main__":
    print("🚀 [Phase 1~3] 로그 분석 및 파서 DB 저장 시작...")
    build_parser()