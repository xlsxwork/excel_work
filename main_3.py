import re
import streamlit as st
import pandas as pd
import gspread
from io import BytesIO
from oauth2client.service_account import ServiceAccountCredentials
from concurrent.futures import ThreadPoolExecutor

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

# --- Обработка данных ---
class DataProcessor:
    @staticmethod
    def load_worksheet(ws):
        try:
            data = ws.get_all_values()
            if not data or len(data) < 2:
                return None
            df = pd.DataFrame(data[1:], columns=data[0])
            df['Лист'] = ws.title
            for col in df.columns:
                df[col] = df[col].astype(str).str.strip()
            return df
        except Exception as e:
            st.error(f"Ошибка загрузки листа '{ws.title}': {e}")
            return None
        
    @staticmethod
    def normalize_text(text):
        text = str(text).lower()
        replacements = {
            'х': 'x', '–': '-', '—': '-', 'ё': 'е',
            'мм2': 'мм²', 'мм^2': 'мм²',
            'см2': 'см²', 'см^2': 'см²',
            'м2': 'м²', 'м^2': 'м²',
        }
        for k, v in replacements.items():
            text = text.replace(k, v)
        text = re.sub(r'(?<=\d)\s+(?=мм²|см²|м²)', '', text)
        return text

    @staticmethod
    def split_preserve_sizes(text):
        text = DataProcessor.normalize_text(text)
        text = re.sub(
            r'(\d+(?:[.,]\d+)?)\s*[xх×*]\s*(\d+(?:[.,]\d+)?)',
            r'\1x\2',
            text
        )
        text = re.sub(r'\bмм\s*[\^]?\s*2\b', 'мм²', text)
        text = re.sub(r'\bсм\s*[\^]?\s*2\b', 'см²', text)
        text = re.sub(r'\bм\s*[\^]?\s*2\b', 'м²', text)
        return re.findall(r'\d+(?:[.,]\d+)?x\d+(?:[.,]\d+)?|мм²|см²|м²|\w+', text)

    @staticmethod
    def match_query(row_text, query_words, require_all=False):
        row_words = DataProcessor.split_preserve_sizes(row_text)
        match_count = sum(1 for word in query_words if word in row_words)
        return match_count if not require_all or match_count == len(query_words) else 0

# --- UI ---
class UIComponents:
    @staticmethod
    def setup_page():
        st.set_page_config(
            page_title=AppConfig.PAGE_TITLE,
            layout=AppConfig.PAGE_LAYOUT
        )
        st.title(AppConfig.PAGE_TITLE)

    @staticmethod
    def show_results(results, selected_columns):
        if not results.empty:
            results = results.reset_index(drop=True)
            results.index = results.index + 2
            results.index.name = "№ строки"
            
            results_with_index = results.reset_index()
            
            if selected_columns:
                columns_to_show = [col for col in selected_columns if col in results.columns]
                filtered_results = results_with_index[columns_to_show]
            else:
                filtered_results = results_with_index

            st.dataframe(
                filtered_results,
                use_container_width=True,
                hide_index=False,
                column_config={
                    "№ строки": st.column_config.NumberColumn(
                        "№ строки",
                        help="Номер строки в исходной таблице",
                        width="small"
                    )
                }
            )

            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                filtered_results.to_excel(writer, index=False, sheet_name='Результаты')
            
            excel_buffer.seek(0)
            
            st.download_button(
                label="⬇️ Скачать результаты в Excel",
                data=excel_buffer,
                file_name="search_results.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

# --- Основное приложение ---
class GoogleSheetSearchApp:
    def __init__(self):
        try:
            UIComponents.setup_page()
            self.client = GoogleSheetsConnector.get_client()
            self.initialize_session_state()
            self.authenticate()
        except Exception as e:
            st.error(f"Ошибка инициализации: {str(e)}")
            st.stop()

    def initialize_session_state(self):
        if 'combined_df' not in st.session_state:
            st.session_state.combined_df = None
        if 'sheet_id' not in st.session_state:
            st.session_state.sheet_id = None
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False
        if 'available_sheets' not in st.session_state:
            st.session_state.available_sheets = []
        if 'data_loaded' not in st.session_state:
            st.session_state.data_loaded = False
        if 'search_column' not in st.session_state:
            st.session_state.search_column = None

    def authenticate(self):
        if not st.session_state.authenticated:
            try:
                correct_password = AppConfig.get_password()
            except ValueError as e:
                st.error(f"Ошибка конфигурации: {str(e)}")
                return
                
            col1, _, _, _ = st.columns([1, 1, 1, 1])
            with col1:
                password = st.text_input("🔒 Введите пароль для доступа", 
                                      type="password",
                                      key="password_input")
                if st.button("Войти", key="login_button"):
                    if password == correct_password:
                        st.session_state.authenticated = True
                        self.load_available_sheets()
                        st.rerun()
                    else:
                        st.error("❌ Неверный пароль")
            return
        self.show_main_app()

    def load_available_sheets(self):
        try:
            sheets = self.client.openall()
            st.session_state.available_sheets = [
                {
                    'title': sheet.title,
                    'url': f"https://docs.google.com/spreadsheets/d/{sheet.id}/edit#gid={sheet.sheet1.id}",
                    'id': sheet.id
                }
                for sheet in sheets
            ]
        except Exception as e:
            st.error(f"Ошибка при загрузке списка таблиц: {str(e)}")
            st.session_state.available_sheets = []

    def process_sheets(self, spreadsheet):
        with ThreadPoolExecutor() as executor:
            dfs = list(executor.map(DataProcessor.load_worksheet, spreadsheet.worksheets()))
        return [df for df in dfs if df is not None]

    def load_data(self, sheet_url):
        try:
            sheet_id = GoogleSheetsConnector.extract_sheet_id(sheet_url)
            if not sheet_id:
                st.error("❌ Некорректная ссылка на Google Таблицу")
                return False

            if st.session_state.sheet_id != sheet_id:
                with st.spinner("Загрузка данных..."):
                    spreadsheet = self.client.open_by_key(sheet_id)
                    all_data = self.process_sheets(spreadsheet)

                    if not all_data:
                        st.warning("⚠️ В таблице нет данных")
                        return False

                    st.session_state.combined_df = pd.concat(all_data, ignore_index=True)
                    st.session_state.sheet_id = sheet_id
                    st.session_state.data_loaded = True
                    st.session_state.search_column = None
                    st.success(f"✅ Данные успешно загружены. Записей: {len(st.session_state.combined_df)}")
            return True
            
        except gspread.exceptions.APIError as e:
            st.error(f"❌ Ошибка доступа: {str(e)}")
            st.error("Проверьте доступ сервисного аккаунта к таблице")
            return False
        except Exception as e:
            st.error(f"❌ Неожиданная ошибка: {str(e)}")
            return False

    def show_main_app(self):
        if st.session_state.available_sheets:
            st.subheader("📂 Доступные Google Таблицы")
            cols = st.columns(3)
            col_index = 0
            
            for sheet in st.session_state.available_sheets:
                with cols[col_index]:
                    with st.container(border=True):
                        st.markdown(f"**{sheet['title']}**")
                        st.markdown(f"[Открыть таблицу]({sheet['url']})")
                        if st.button(f"Выбрать {sheet['title']}", key=f"select_{sheet['id']}"):
                            st.session_state.sheet_url = sheet['url']
                            st.rerun()
                col_index = (col_index + 1) % 3
            st.divider()
        
        sheet_url = st.text_input(
            "📎 Вставьте ссылку на Google Таблицу",
            value=st.session_state.get('sheet_url', ''),
            key="sheet_url",
            help="Пример: https://docs.google.com/spreadsheets/d/ID_ТАБЛИЦЫ/edit#gid=ID_ЛИСТА"
        )
        
        if st.button("🔄 Загрузить данные", key="load_data_button"):
            if self.load_data(sheet_url):
                st.rerun()

        if st.session_state.data_loaded and st.session_state.combined_df is not None:
            combined_df = st.session_state.combined_df
            
            if st.session_state.search_column is None:
                st.session_state.search_column = combined_df.columns[0]
            
            selected_column = st.selectbox(
                "📁 Выберите колонку для поиска",
                combined_df.columns,
                index=list(combined_df.columns).index(st.session_state.search_column),
                key="column_select"
            )
            
            st.session_state.search_column = selected_column

            all_columns = ['Лист'] + [col for col in combined_df.columns if col != 'Лист']
            selected_columns = st.multiselect(
                "📋 Выберите колонки для вывода",
                options=all_columns,
                default=all_columns[:3] if len(all_columns) > 3 else all_columns,
                key="output_columns"
            )

            search_query = st.text_input("🔎 Введите слово или часть слова для поиска", key="search_query")

            exact_match = st.checkbox("🧩 Только полное совпадение всех слов", value=True, key="exact_match")
            partial_match = st.checkbox("🔍 Частичное совпадение", key="partial_match")

            if st.button("🔍 Найти", key="search_button") and search_query:
                with st.spinner("Поиск..."):
                    query_words = DataProcessor.split_preserve_sizes(search_query)
                    require_all = exact_match and not partial_match
                    
                    search_df = combined_df.copy()
                    search_df['__match_count'] = search_df[selected_column].apply(
                        lambda text: DataProcessor.match_query(text, query_words, require_all=require_all)
                    )

                    results = search_df[search_df['__match_count'] > 0]
                    results = results.sort_values(by='__match_count', ascending=False)
                    results = results.drop(columns='__match_count')

                    st.success(f"🔎 Найдено: {len(results)} записей")
                    UIComponents.show_results(results, selected_columns)

if __name__ == "__main__":
    GoogleSheetSearchApp()