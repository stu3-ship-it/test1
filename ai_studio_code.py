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
    SHEET_URL = "https://docs.google.com/spreadsheets/d/11BXtN3aevJls6Q2IR_IbT80-9XvhBkjbTCgANmsxqkg/edit"
    
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
except Exception as e:
    st.error("❌ 系統發生未預期錯誤，請通知管理員。")
    print(traceback.format_exc())  

st.error("測試線")
print(traceback.format_exc())
