import pandas as pd
import requests
from urllib.parse import urlparse, parse_qs, unquote, unquote_plus
from datetime import datetime
import getpass
import time
import urllib3
import warnings
from typing import Optional, Any, List

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

class DashboardstroiClient:
    """Клиент для работы с dashboard-stroi.mos.ru"""

    def __init__(self, login, password):
        self.login = login
        self.password = password
        self.session = requests.Session()
        self.token = None
        self.xsrf_token = None
        self.base_url = "https://dashboard-stroi.mos.ru"

    def authorize(self) -> bool:
        """Авторизация через СУДИР OAuth"""
        # 1. Инициализация СУДИР
        auth_url = "https://sudir.mos.ru/blitz/login/methods/password?bo=%2Fblitz%2Foauth%2Fae%3Fclient_id%3Ddashboard-stroi.mos.ru%26redirect_uri%3Dhttps%253A%252F%252Fdashboard-stroi.mos.ru%252Foauth%252Flogin-internal%26response_type%3Dcode%26scope%3Dopenid%2Bprofile%2Bemail%2Bemployee%2Bgroups%26state%3DENZPq1I93b184s6FxZzvh4yOwKZN5eyhjDBDJsfg%26access_type%3Doffline"


        self.session.get(auth_url)

        # 2. Проверка аккаунтов
        self.session.post(
            "https://sudir.mos.ru/blitz/login/mus",
            headers={"X-Requested-With": "XMLHttpRequest"},
        )

        # 3. Авторизация
        login_response = self.session.post(
            "https://sudir.mos.ru/blitz/login/methods/password",
            data={"isDelayed": "false", "login": self.login, "password": self.password},
            allow_redirects=True,
        )

        # 4. Проверка успешности
        if "dashboard-stroi.mos.ru" not in login_response.url:
            raise Exception("Ошибка авторизации")

        # 5. Извлечение токена
        parsed = urlparse(login_response.url)
        token_param = parse_qs(parsed.query).get("token", [None])[0]

        if not token_param:
            raise Exception("Токен не найден в URL")

        self.token = unquote(token_param)

        # 6. XSRF токен
        xsrf = self.session.cookies.get("XSRF-TOKEN")
        if xsrf:
            self.xsrf_token = unquote_plus(xsrf)

        return True

    def get(self, path, **kwargs) -> requests.Response:
        """GET запрос к API или фронтовому эндпоинту"""
        headers = kwargs.pop("headers", {})

        # Добавляем токены если есть
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if self.xsrf_token:
            headers["X-XSRF-TOKEN"] = self.xsrf_token

        headers.setdefault("Accept", "application/json, text/html, */*")
        headers.setdefault("X-Requested-With", "XMLHttpRequest")

        url = f"{self.base_url}{path}" if path.startswith("/") else path
        response = self.session.get(url, headers=headers, **kwargs)

        return response

    def post(self, path, data=None, json=None, **kwargs) -> requests.Response:
        """POST запрос"""
        headers = kwargs.pop("headers", {})

        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if self.xsrf_token:
            headers["X-XSRF-TOKEN"] = self.xsrf_token

        headers.setdefault("Accept", "application/json")
        headers.setdefault("X-Requested-With", "XMLHttpRequest")

        url = f"{self.base_url}{path}" if path.startswith("/") else path
        response = self.session.post(
            url, data=data, json=json, headers=headers, **kwargs
        )

        return response

    def get_page(self, path) -> str:
        """Получить HTML страницу (для парсинга)"""
        response = self.get(path)
        return response.text

    def get_json(self, path) -> Optional[Any]:
        """Получить JSON из API"""
        response = self.get(path)
        if response.status_code == 200:
            try:
                return response.json()
            except Exception:
                return None
        return None

    def get_catalog_objects(self) -> List[Any]:
        """
        Получить список объектов из каталога
        Возвращает список всех объектов с их данными
        """
        response = self.get("/api/catalog")

        if response.status_code == 200:
            data = response.json()
            return data.get("objects", {}).get("data", [])

        return []

    def get_catalog_ids(self):
        """
        Получить только ID всех объектов из каталога
        Возвращает список: [1257, 1258, 1259, ...]
        """
        objects = self.get_catalog_objects()
        return [obj["id"] for obj in objects if "id" in obj]

    def get_dashboard_data(self, object_id) -> Optional[Any]:
        """Получить данные dashboard для объекта"""
        response = self.get(f"/api/dashboard/{object_id}")

        if response.status_code == 200:
            try:
                return response.json()
                
            except Exception as e:
                print('Нет данных', e)

    def extract_rows(self, data):
        rows = []
        
        # Получаем базовые данные объекта
        obj_id = data.get('object', {}).get('id')
        obj_name = data.get('object', {}).get('name')
        obj_uin = data.get('object', {}).get('uin')
        
        # Проходим по constructionStagesData
        for stage in data.get('constructionStagesData', []):
            oiv = stage.get('oiv', {})
            
            # Основная строка из stage
            row = {
                'id': obj_id,
                'name': obj_name,
                'uin': obj_uin,
                'view_name': stage.get('view_name'),
                'fact': oiv.get('fact'),
                'plan': oiv.get('plan'),
                'fact_delta_week': oiv.get('delta_fact_week'),
                'fact_delta_month': oiv.get('delta_fact_month'),
                'plan_delta_week': oiv.get('delta_plan_week'),
                'plan_delta_month': oiv.get('delta_plan_month'),
                'link': f'https://dashboard-stroi.mos.ru/dashboard/{obj_id}'
            }
            rows.append(row)
            
            # Обрабатываем children
            children = stage.get('children', [])
            
            if isinstance(children, list):
                for child in children:
                    if isinstance(child, dict) and 'view_name' in child and 'fact_oiv' in child:
                        child_row = {
                            'id': obj_id,
                            'name': obj_name,
                            'uin': obj_uin,
                            'view_name': child.get('view_name'),
                            'fact': child.get('fact_oiv'),
                            'plan': None,
                            'fact_delta_week': None,
                            'fact_delta_month': None,
                            'plan_delta_week': None,
                            'plan_delta_month': None,
                            'link': f'https://dashboard-stroi.mos.ru/dashboard/{obj_id}'
                        }
                        rows.append(child_row)
            
            elif isinstance(children, dict):
                for child in children.values():
                    if isinstance(child, dict) and 'view_name' in child and 'fact_oiv' in child:
                        child_row = {
                            'id': obj_id,
                            'name': obj_name,
                            'uin': obj_uin,
                            'view_name': child.get('view_name'),
                            'fact': child.get('fact_oiv'),
                            'plan': None,
                            'fact_delta_week': None,
                            'fact_delta_month': None,
                            'plan_delta_week': None,
                            'plan_delta_month': None,
                            'link': f'https://dashboard-stroi.mos.ru/dashboard/{obj_id}'
                        }
                        rows.append(child_row)
        
        return rows

if __name__ == '__main__':
    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        sudir_login = input("Введите логин СУДИР:\n")
        sudir_password = getpass.getpass("Введите пароль СУДИР:\n")
        dashboard_client = DashboardstroiClient(sudir_login, sudir_password)
        try:
            dashboard_client.authorize()
            print("Логин и пароль для СУИД верны")
            break
        except Exception as e:
            print(f'Ошибка авторизации: {e}')
            if attempt < max_attempts:
                print(f'Попытка {attempt}/{max_attempts}. Попробуйте снова.\n')
            else:
                print('Превышено количество попыток!')
                input(' ')
                raise

    all_rows = []
    objects = dashboard_client.get_catalog_ids()
    for idx, value in enumerate(objects):
        data = dashboard_client.get_dashboard_data(value)
        all_rows.extend(dashboard_client.extract_rows(data))
        print(f'{value} обработан, ещё {len(objects) - idx - 1} объектов')
        time.sleep(0.5)
        
    result = pd.DataFrame(all_rows)
    result = result.rename(columns={'name': 'объект', 'view_name': 'объект', 'fact': 'факт', 'plan': 'план',
                        'fact_delta_week' : 'факт_неделя', 'fact_delta_month': 'факт_месяц', 'plan_delta_week' : 'план_неделя',
                        'plan_delta_month' : 'план_месяц', 'link' : 'ссылка'})
    result.to_excel(f'plan_fact_{datetime.today().date()}.xlsx', index=False)
    input('Готово!')
