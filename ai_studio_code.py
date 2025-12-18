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
#import gspread
#from oauth2client.service_account import ServiceAccountCredentials
#from googleapiclient.discovery import build
#from googleapiclient.http import MediaIoBaseUpload
#from streamlit.runtime.scriptrunner import add_script_run_ctx

# --- 1. 網頁設定 ---
st.set_page_config(page_title="抄襲是不對的行為", layout="wide", page_icon="🌞")
