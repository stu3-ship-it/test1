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
    # 3. 主程式介面
    # ==========================================
    SYSTEM_CONFIG = load_settings()
    ROSTER_DICT = load_roster_dict()
    INSPECTOR_LIST = load_inspector_list()
    TEACHER_MAILS = load_teacher_emails()
    
    all_classes, structured_classes = load_sorted_classes()
    if not all_classes:
        all_classes = ["測試班級"]
        structured_classes = [{"grade": "其他", "name": "測試班級"}]

    grades = sorted(list(set([c["grade"] for c in structured_classes])))

    def get_week_num(d):
        try:
            start = datetime.strptime(SYSTEM_CONFIG["semester_start"], "%Y-%m-%d").date()
            if isinstance(d, datetime): d = d.date()
            return max(0, ((d - start).days // 7) + 1)
        except: return 0

    now_tw = datetime.now(TW_TZ)
    today_tw = now_tw.date()

    st.sidebar.title("🏫 功能選單")
    app_mode = st.sidebar.radio("請選擇模式", ["糾察底家👀", "班級負責人🥸", "組長ㄉ窩💃"])

    with st.sidebar.expander("🔧 系統連線診斷", expanded=False):
        if get_gspread_client(): 
            st.success("✅ Google Sheets 連線正常")
        else: 
            st.error("❌ Sheets 連線失敗")
            
        if "gcp_service_account" in st.secrets: 
            st.success("✅ GCP 憑證已讀取")
        else: 
            st.error("⚠️ 未設定 GCP Service Account")
            
        if "system_config" in st.secrets and "drive_folder_id" in st.secrets["system_config"]:
            st.success("✅ Drive 資料夾 ID 已設定")
        else:
            st.warning("⚠️ 未設定 Drive 資料夾 ID")

except Exception as e:
    st.error("❌ 系統發生未預期錯誤，請通知管理員。")
    print(traceback.format_exc())  
