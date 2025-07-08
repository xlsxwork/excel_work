import re
import streamlit as st
import pandas as pd
import gspread
from io import BytesIO
from oauth2client.service_account import ServiceAccountCredentials
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# --- Конфигурация через Streamlit Secrets ---
class AppConfig:
    PAGE_TITLE = "🔍 Поиск по Google Таблице"
    PAGE_LAYOUT = "wide"

    @staticmethod
    def get_credentials():
        if not st.secrets:
            raise ValueError("Secrets не загружены")
        return {
            "type": st.secrets["google"]["type"],
            "project_id": st.secrets["google"]["project_id"],
            "private_key_id": st.secrets["google"]["private_key_id"],
            "private_key": st.secrets["google"]["private_key"].replace('\\n', '\n'),
            "client_email": st.secrets["google"]["client_email"],
            "client_id": st.secrets["google"]["client_id"],
            "auth_uri": st.secrets["google"]["auth_uri"],
            "token_uri": st.secrets["google"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["google"]["auth_provider_x509_cert_url"],
            "client_x509_cert_url": st.secrets["google"]["client_x509_cert_url"],
            "universe_domain": st.secrets["google"].get("universe_domain", "googleapis.com")
        }

    @staticmethod
    def get_password():
        if not st.secrets.get("app"):
            raise ValueError("Не настроены секреты приложения")
        if not st.secrets["app"].get("password"):
            raise ValueError("Пароль не установлен в секретах")
        return st.secrets["app"]["password"]

# --- Работа с Google Sheets ---
class GoogleSheetsConnector:
    @staticmethod
    @st.cache_resource
    def get_client():
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            AppConfig.get_credentials(),
            [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
        )
        return gspread.authorize(creds)

    @staticmethod
    def extract_sheet_id(url):
        patterns = [
            r"/d/([a-zA-Z0-9-_]+)",
            r"spreadsheets/d/([a-zA-Z0-9-_]+)",
            r"^([a-zA-Z0-9-_]+)$"
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

# --- Инициализация состояния ---
def init_state():
    for key, value in {
        'combined_df': None,
        'sheet_id': None,
        'authenticated': False,
        'available_sheets': [],
        'data_loaded': False,
        'sheet_names': [],
        'search_results': None,
        'sheet_url': '',
        'loading_data': False,
        'searching': False
    }.items():
        if key not in st.session_state:
            st.session_state[key] = value

# --- Загрузка доступных таблиц ---
def load_available_sheets(client):
    try:
        sheets = client.openall()
        st.session_state.available_sheets = [{
            'title': s.title,
            'url': f"https://docs.google.com/spreadsheets/d/{s.id}",
            'id': s.id
        } for s in sheets]
    except Exception as e:
        st.error(f"Ошибка получения таблиц: {e}")

# --- Загрузка данных из таблицы ---
def load_data(client, url):
    try:
        sheet_id = GoogleSheetsConnector.extract_sheet_id(url)
        spreadsheet = client.open_by_key(sheet_id)
        worksheets = spreadsheet.worksheets()
        dfs = []
        for ws in worksheets:
            values = ws.get_all_values()
            if values and len(values) > 1:
                df = pd.DataFrame(values[1:], columns=values[0])
                df['Лист'] = ws.title
                dfs.append(df)
        if not dfs:
            return False
        st.session_state.combined_df = pd.concat(dfs, ignore_index=True)
        st.session_state.sheet_id = sheet_id
        st.session_state.data_loaded = True
        st.session_state.sheet_names = [ws.title for ws in worksheets]
        return True
    except Exception as e:
        st.error(f"Ошибка загрузки данных: {e}")
        return False

# --- Основной UI ---
def main():
    st.set_page_config(page_title=AppConfig.PAGE_TITLE, layout=AppConfig.PAGE_LAYOUT)
    init_state()
    st.title(AppConfig.PAGE_TITLE)

    client = GoogleSheetsConnector.get_client()

    if not st.session_state.authenticated:
        try:
            correct_password = AppConfig.get_password()
        except ValueError as e:
            st.error(f"Ошибка: {e}")
            return
        pw = st.text_input("🔐 Введите пароль", type="password")
        if st.button("Войти"):
            if pw == correct_password:
                st.session_state.authenticated = True
                load_available_sheets(client)
                st.rerun()
            else:
                st.error("❌ Неверный пароль")
        return

    if st.session_state.loading_data:
        with st.spinner("Загрузка данных..."):
            if load_data(client, st.session_state.sheet_url):
                st.session_state.loading_data = False
                st.rerun()
        return

    st.subheader("📂 Доступные Google Таблицы")
    cols = st.columns(3)
    for i, sheet in enumerate(st.session_state.available_sheets):
        with cols[i % 3]:
            st.markdown(f"**{sheet['title']}**")
            st.markdown(f"[Открыть таблицу]({sheet['url']})")
            if st.button(f"Выбрать {sheet['title']}", key=f"select_{sheet['id']}"):
                st.session_state.sheet_url = sheet['url']
                st.session_state.loading_data = True
                st.rerun()

    if st.session_state.data_loaded:
        st.success(f"✅ Данные загружены. Записей: {len(st.session_state.combined_df)}")

if __name__ == "__main__":
    main()
