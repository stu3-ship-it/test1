import streamlit as st
import pandas as pd
import os
import smtplib
import time
import io
import traceback
import threading
import uuid
import re
import sqlite3
import json
import random
import concurrent.futures
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, date, timedelta
from datetime import timezone
import pytz
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from streamlit.runtime.scriptrunner import add_script_run_ctx

# --- 1. 網頁設定 ---
st.set_page_config(page_title="抄襲是不對的行為", layout="wide", page_icon="☘️")

# --- 2. 捕捉全域錯誤 ---
try:
    # ==========================================
    # 0. 基礎設定與時區
    # ==========================================
    TW_TZ = pytz.timezone('Asia/Taipei')

    MAX_IMAGE_BYTES = 10 * 1024 * 1024  # 單檔圖片 10MB 上限
    QUEUE_DB_PATH = "task_queue_v4_wal.db"
    
    # Google Sheet 網址
    SHEET_URL = "https://docs.google.com/spreadsheets/d/108HJ47lwEzHrJ7I0-olC3S-oMZBIF60-55fQClqjIBw/edit"
    
    SHEET_TABS = {
        "main": "main_data", 
        "settings": "settings",
        "roster": "roster",
        "inspectors": "inspectors",
        "duty": "duty",
        "teachers": "teachers",
        "appeals": "appeals"
    }

    EXPECTED_COLUMNS = [
        "日期", "週次", "班級", "評分項目", "檢查人員",
        "內掃原始分", "外掃原始分", "垃圾原始分", "垃圾內掃原始分", "垃圾外掃原始分", "晨間打掃原始分", "手機人數",
        "備註", "違規細項", "照片路徑", "登錄時間", "修正", "晨掃未到者", "紀錄ID"
    ]

    APPEAL_COLUMNS = [
        "申訴日期", "班級", "違規日期", "違規項目", "原始扣分", "申訴理由", "佐證照片", "處理狀態", "登錄時間", "對應紀錄ID"
    ]

    # ==========================================
    # 1. Google 連線整合
    # ==========================================

    @st.cache_resource
    def get_credentials():
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        if "gcp_service_account" not in st.secrets:
            st.error("❌ 找不到 secrets 設定")
            return None
        creds_dict = dict(st.secrets["gcp_service_account"])
        return ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

    @st.cache_resource
    def get_gspread_client():
        try:
            creds = get_credentials()
            if not creds: return None
            return gspread.authorize(creds)
        except Exception as e:
            st.error(f"❌ Google Sheet 連線失敗: {e}")
            return None

    @st.cache_resource
    def get_drive_service():
        try:
            creds = get_credentials()
            if not creds: return None
            return build('drive', 'v3', credentials=creds, cache_discovery=False)
        except Exception as e:
            st.warning(f"⚠️ Google Drive 連線失敗: {e}")
            return None

    @st.cache_resource(ttl=21600)
    def get_spreadsheet_object():
        client = get_gspread_client()
        if not client: return None
        try: return client.open_by_url(SHEET_URL)
        except Exception as e: st.error(f"❌ 無法開啟試算表: {e}")
        return None

    def get_worksheet(tab_name):
        max_retries = 3
        wait_time = 2
        sheet = get_spreadsheet_object()
        if not sheet: return None
        for attempt in range(max_retries):
            try:
                try: return sheet.worksheet(tab_name)
                except gspread.WorksheetNotFound:
                    cols = 20 if tab_name != "appeals" else 15
                    ws = sheet.add_worksheet(title=tab_name, rows=100, cols=cols)
                    if tab_name == "appeals": ws.append_row(APPEAL_COLUMNS)
                    return ws
            except Exception as e:
                if "429" in str(e): 
                    time.sleep(wait_time * (attempt + 1))
                    continue
                else: 
                    print(f"❌ 讀取分頁 '{tab_name}' 失敗: {e}")
                    return None
        return None

    def upload_image_to_drive(file_obj, filename):
        def _upload_action():
            service = get_drive_service()
            if not service: raise Exception("Drive Service Init Failed")
            
            folder_id = st.secrets["system_config"].get("drive_folder_id")
            if not folder_id: raise Exception("No Drive Folder ID")

            file_metadata = {'name': filename, 'parents': [folder_id]}
            media = MediaIoBaseUpload(file_obj, mimetype='image/jpeg')
            
            file = service.files().create(
                body=file_metadata, media_body=media, fields='id', supportsAllDrives=True
            ).execute(num_retries=1)
            
            try:
                service.permissions().create(fileId=file.get('id'), body={'role': 'reader', 'type': 'anyone'}).execute()
            except: pass 
            return f"https://drive.google.com/thumbnail?id={file.get('id')}&sz=w1000"

        try:
            return execute_with_retry(_upload_action)
        except Exception as e:
            print(f"⚠️ Drive 上傳最終失敗: {str(e)}")
            return None

    def clean_id(val):
        try:
            if pd.isna(val) or val == "": return ""
            return str(int(float(val))).strip()
        except: return str(val).strip()

    # ==========================================
    # 圖片暫存資料夾
    # ==========================================
    IMG_DIR = "evidence_photos"
    os.makedirs(IMG_DIR, exist_ok=True)

    # ==========================================
    # SQLite 背景佇列系統 (SRE Hardened + ThreadPool)
    # ==========================================
    _queue_lock = threading.Lock()

    @st.cache_resource
    def get_queue_connection():
        conn = sqlite3.connect(
            QUEUE_DB_PATH, 
            check_same_thread=False, 
            timeout=30.0, 
            isolation_level="IMMEDIATE" 
        )
        
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=30000;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except:
            pass

        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_queue (
                id TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                created_ts TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_status_created ON task_queue (status, created_ts);")
        conn.commit()
        return conn

    def enqueue_task(task_type: str, payload: dict) -> str:
        conn = get_queue_connection()
        task_id = str(uuid.uuid4())
        created_ts = datetime.now(timezone.utc).isoformat()
        payload_json = json.dumps(payload, ensure_ascii=False)

        with _queue_lock:
            conn.execute(
                "INSERT INTO task_queue (id, task_type, created_ts, payload_json, status, attempts, last_error) "
                "VALUES (?, ?, ?, ?, 'PENDING', 0, NULL)",
                (task_id, task_type, created_ts, payload_json)
            )
            conn.commit()
        return task_id

    def get_queue_metrics():
        conn = get_queue_connection()
        metrics = {"pending": 0, "retry": 0, "failed": 0, "oldest_pending_sec": 0, "recent_errors": []}
        with _queue_lock:
            cur = conn.cursor()
            cur.execute("SELECT status, COUNT(*) FROM task_queue GROUP BY status")
            rows = cur.fetchall()
            for status, count in rows:
                if status == 'PENDING': metrics["pending"] = count
                elif status == 'RETRY': metrics["retry"] = count
                elif status == 'FAILED': metrics["failed"] = count
            
            cur.execute("SELECT MIN(created_ts) FROM task_queue WHERE status IN ('PENDING', 'RETRY')")
            oldest_ts_str = cur.fetchone()[0]
            
            cur.execute("SELECT last_error, created_ts FROM task_queue WHERE status='FAILED' OR status='RETRY' ORDER BY created_ts DESC LIMIT 5")
            metrics["recent_errors"] = cur.fetchall()

        if oldest_ts_str:
            try:
                created = datetime.fromisoformat(oldest_ts_str.replace("Z", "+00:00"))
                now = datetime.now(pytz.utc)
                metrics["oldest_pending_sec"] = (now - created).total_seconds()
            except: pass
        return metrics

    def fetch_next_task(max_attempts: int = 6):
        conn = get_queue_connection()
        with _queue_lock:
            try:
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT id, task_type, created_ts, payload_json, status, attempts, last_error
                    FROM task_queue
                    WHERE status IN ('PENDING', 'RETRY')
                    AND attempts < ?
                    ORDER BY created_ts ASC
                    LIMIT 1
                    """,
                    (max_attempts,)
                )
                row = cur.fetchone()
                
                if not row:
                    conn.commit()
                    return None

                task_id, task_type, created_ts, payload_json, status, attempts, last_error = row
                
                cur.execute(
                    "UPDATE task_queue SET status = 'IN_PROGRESS', attempts = attempts + 1 WHERE id = ? AND status = ?",
                    (task_id, status)
                )
                
                if cur.rowcount == 0:
                    conn.commit()
                    return None

                conn.commit()
                
                try: payload = json.loads(payload_json)
                except: payload = {}
                    
                return {
                    "id": task_id,
                    "task_type": task_type,
                    "created_ts": created_ts,
                    "payload": payload,
                    "status": "IN_PROGRESS",
                    "attempts": attempts + 1,
                    "last_error": last_error,
                }
            except Exception as e:
                try: conn.rollback()
                except: pass
                return None

    def update_task_status(task_id: str, status: str, attempts: int, last_error: str | None):
        conn = get_queue_connection()
        with _queue_lock:
            conn.execute(
                "UPDATE task_queue SET status = ?, attempts = ?, last_error = ? WHERE id = ?",
                (status, attempts, last_error, task_id),
            )
            conn.commit()
    
    def requeue_stuck_tasks(stale_seconds=600):
        # 簡單實作：目前策略依賴重試與 attempts 限制
        pass 

    def _exp_backoff_seconds(attempts: int) -> float:
        base = 2.0
        cap = 60.0
        return random.uniform(1.0, min(cap, base * (2 ** max(0, attempts))))


except Exception as e:
    st.error("❌ 系統發生未預期錯誤，請通知管理員。")
    print(traceback.format_exc())  

st.error("測試線")
print(traceback.format_exc())
