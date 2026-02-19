@app.route('/run/remote_web')
def run_remote_web():
    import docker
    client = docker.from_env()
    try:
        # 1. IP(172.19.0.3) 또는 이름(ueba-webserver)으로 컨테이너 식별
        container = client.containers.get("ueba-webserver")
        
        # 2. 원격 컨테이너 내부의 독립된 스크립트 실행
        exit_code, output = container.exec_run(
            "python3 /UEBA_WEB/tools/gen.py",
            workdir="/UEBA_WEB"
        )
        
        return jsonify({
            "status": "success", 
            "output": f"🌐 원격 서버(172.19.0.3) 응답:\n{output.decode('utf-8')}"
        })
    except Exception as e:
        return jsonify({"status": "error", "output": str(e)})