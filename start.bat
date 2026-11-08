@echo off
cd /d D:\wowlogsparser
start http://127.0.0.1:8080
python -m uvicorn wcl_archiver.api.app:create_app --factory --reload --host 127.0.0.1 --port 8080
