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

    @staticmethod
    def extract_price_columns(df):
        price_cols = []
        date_pattern = re.compile(r'Цена\s*\n?\s*\d{4}-\d{2}-\d{2}')
        
        for col in df.columns:
            if col.lower().startswith('цена') or date_pattern.search(col):
                price_cols.append(col)
        
        return price_cols
    
    @staticmethod
    def sort_price_columns(price_columns):
        date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
        
        dated_cols = []
        for col in price_columns:
            match = date_pattern.search(col)
            if match:
                date_str = match.group()
                dated_cols.append((col, datetime.strptime(date_str, '%Y-%m-%d')))
        
        if dated_cols:
            dated_cols.sort(key=lambda x: x[1], reverse=True)
            return [col[0] for col in dated_cols]
        
        return price_columns

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
    def show_sheet_sources(sheet_names):
        st.markdown("### 📌 Данные собираются со следующих сайтов:")
        
        card_style = """
            display: inline-block;
            margin: 6px;
            padding: 10px 18px;
            background-color: #d43f3a;
            color: white;
            border-radius: 12px;
            font-weight: 600;
            font-size: 0.95rem;
            box-shadow: 0 2px 6px rgba(0, 0, 0, 0.2);
        """

        html = "<div style='margin-top: 10px;'>"
        for name in sheet_names:
            html += f"<div style='{card_style}'>{name}</div>"
        html += "</div>"

        st.markdown(html, unsafe_allow_html=True)

    @staticmethod
    def show_results(results, selected_columns, latest_price_col=None):
        if not results.empty:
            results = results.reset_index(drop=True)
            results.index = results.index + 2
            results.index.name = "№ строки"
            
            results_with_index = results.reset_index()
            
            if selected_columns:
                columns_to_show = [col for col in selected_columns if col in results.columns]
                
                if latest_price_col and latest_price_col in columns_to_show:
                    columns_to_show = [
                        f"Цена актуальная ({latest_price_col})" if col == latest_price_col else col 
                        for col in columns_to_show
                    ]
                    results_with_index = results_with_index.rename(
                        columns={latest_price_col: f"Цена актуальная ({latest_price_col})"}
                    )
                
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
        session_defaults = {
            'combined_df': None,
            'sheet_id': None,
            'authenticated': False,
            'available_sheets': [],
            'data_loaded': False,
            'search_column': "Название",
            'sheet_names': [],
            'price_columns': [],
            'latest_price_col': None,
            'search_triggered': False,
            'search_results': None,
            'sheets_loaded': False,
            'need_load': False
        }
        
        for key, value in session_defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value

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
                if st.button("Войти", key="login_button") or password:
                    if password == correct_password:
                        st.session_state.authenticated = True
                        st.session_state.sheets_loaded = False
                        st.rerun()
                    elif password:
                        st.error("❌ Неверный пароль")
            return
        self.show_main_app()

    def load_available_sheets(self):
        try:
            with st.spinner("Поиск доступных таблиц..."):
                sheets = self.client.openall()
                if not sheets:
                    st.warning("Не найдено ни одной доступной таблицы")
                    st.session_state.available_sheets = []
                else:
                    st.session_state.available_sheets = [
                        {
                            'title': sheet.title,
                            'url': f"https://docs.google.com/spreadsheets/d/{sheet.id}",
                            'id': sheet.id
                        }
                        for sheet in sheets
                    ]
                    st.session_state.sheets_loaded = True
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

            if st.session_state.sheet_id != sheet_id or not st.session_state.data_loaded:
                with st.spinner("Загрузка данных..."):
                    spreadsheet = self.client.open_by_key(sheet_id)
                    all_data = self.process_sheets(spreadsheet)
                    sheet_names = [ws.title for ws in spreadsheet.worksheets()]

                    if not all_data:
                        st.warning("⚠️ В таблице нет данных")
                        return False

                    st.session_state.combined_df = pd.concat(all_data, ignore_index=True)
                    st.session_state.sheet_id = sheet_id
                    st.session_state.data_loaded = True
                    st.session_state.sheet_names = sheet_names
                    
                    price_columns = DataProcessor.extract_price_columns(st.session_state.combined_df)
                    st.session_state.price_columns = DataProcessor.sort_price_columns(price_columns)
                    
                    if st.session_state.price_columns:
                        st.session_state.latest_price_col = st.session_state.price_columns[0]
                    
                    st.success(f"✅ Данные успешно загружены. Записей: {len(st.session_state.combined_df)}")
            return True
            
        except gspread.exceptions.APIError as e:
            st.error(f"❌ Ошибка доступа: {str(e)}")
            st.error("Проверьте доступ сервисного аккаунта к таблице")
            return False
        except Exception as e:
            st.error(f"❌ Неожиданная ошибка: {str(e)}")
            return False

    def perform_search(self):
        search_query = st.session_state.get('search_query', '')
        if not search_query or not st.session_state.data_loaded or st.session_state.combined_df is None:
            return

        combined_df = st.session_state.combined_df
        selected_column = st.session_state.search_column
        selected_columns = st.session_state.get('output_columns', [])
        exact_match = st.session_state.get('exact_match', True)
        partial_match = st.session_state.get('partial_match', False)

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

            st.session_state.search_results = results
            st.success(f"🔎 Найдено: {len(results)} записей")

    def show_main_app(self):
        # Загружаем список таблиц только один раз
        if not st.session_state.sheets_loaded:
            self.load_available_sheets()
            st.rerun()

        # Показываем доступные таблицы
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
                            st.session_state.data_loaded = False
                            st.session_state.search_results = None
                            if self.load_data(sheet['url']):
                                st.rerun()
                col_index = (col_index + 1) % 3
            st.divider()
        
        # Поле для ввода ссылки
        sheet_url = st.text_input(
            "📎 Вставьте ссылку на Google Таблицу",
            value=st.session_state.get('sheet_url', ''),
            key="sheet_url",
            help="Пример: https://docs.google.com/spreadsheets/d/ID_ТАБЛИЦЫ/edit#gid=ID_ЛИСТА"
        )

        # Кнопка загрузки данных
        if st.button("Загрузить данные", disabled=not sheet_url):
            st.session_state.need_load = True
            st.session_state.search_results = None
            if self.load_data(sheet_url):
                st.rerun()

        # Показываем источники данных
        if st.session_state.data_loaded and st.session_state.sheet_names:
            UIComponents.show_sheet_sources(st.session_state.sheet_names)
            st.divider()

        # Основной функционал поиска
        if st.session_state.data_loaded and st.session_state.combined_df is not None:
            combined_df = st.session_state.combined_df
            
            # Настройки поиска
            col1, col2 = st.columns(2)
            with col1:
                default_index = 0
                if 'Название' in combined_df.columns:
                    default_index = list(combined_df.columns).index('Название')
                elif 'название' in combined_df.columns:
                    default_index = list(combined_df.columns).index('название')
                
                selected_column = st.selectbox(
                    "📁 Выберите колонку для поиска",
                    combined_df.columns,
                    index=default_index,
                    key="column_select"
                )
                st.session_state.search_column = selected_column

            with col2:
                default_columns = ['Лист']
                if 'URL' in combined_df.columns:
                    default_columns.append('URL')
                if 'Название' in combined_df.columns:
                    default_columns.append('Название')
                elif 'название' in combined_df.columns:
                    default_columns.append('название')
                
                if st.session_state.price_columns:
                    default_columns.extend(st.session_state.price_columns)
                
                all_columns = [col for col in combined_df.columns if col != 'Лист']
                all_columns = ['Лист'] + sorted(all_columns)
                
                selected_columns = st.multiselect(
                    "📋 Выберите колонки для вывода",
                    options=all_columns,
                    default=default_columns,
                    key="output_columns"
                )

            # Форма поиска
            with st.form(key='search_form'):
                search_query = st.text_input(
                    "🔎 Введите слово или часть слова для поиска", 
                    key="search_query"
                )

                col3, col4 = st.columns(2)
                with col3:
                    exact_match = st.checkbox(
                        "🧩 Только полное совпадение всех слов", 
                        value=True, 
                        key="exact_match"
                    )
                with col4:
                    partial_match = st.checkbox(
                        "🔍 Частичное совпадение", 
                        key="partial_match"
                    )

                submitted = st.form_submit_button("🔍 Найти")
                if submitted:
                    self.perform_search()
                    st.rerun()

            # Результаты поиска
            if st.session_state.search_results is not None:
                UIComponents.show_results(
                    st.session_state.search_results, 
                    st.session_state.get('output_columns', []), 
                    st.session_state.latest_price_col
                )

if __name__ == "__main__":
    GoogleSheetSearchApp()
