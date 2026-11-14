@echo off
cd /d "%~dp0"
start http://127.0.0.1:8080
python -m uvicorn wcl_archiver.api.app:create_app --factory --host 127.0.0.1 --port 8080
