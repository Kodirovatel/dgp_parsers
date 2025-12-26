import pandas as pd
import requests
from urllib.parse import urlparse, parse_qs, unquote, unquote_plus
from datetime import datetime
import getpass
import urllib3
import warnings
from typing import Optional, Any, List, Dict

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

    def get_catalog_objects(self, oks_status_array_name=None) -> List[Any]:
        """
        Получить список объектов из каталога

        Args:
            oks_status_array_name (str, optional): Значение для параметра oksStatusArrayName (например, "СМР")

        Returns:
            list: Список всех объектов с их данными
        """
        path = "/api/catalog"
        params = {}
        if oks_status_array_name:
            params["oksStatusArrayName"] = oks_status_array_name

        response = self.get(path, params=params)

        if response.status_code == 200:
            data = response.json()
            return data.get("objects", {}).get("data", [])

        return []

    def get_catalog_objects_info(self, oks_status_array_name=None) -> List[Dict[str, Any]]:
        """
        Получить информацию об объектах из каталога (id, name, uin)

        Args:
            oks_status_array_name (str, optional): Значение для параметра oksStatusArrayName

        Returns:
            list[dict]: Список словарей с ключами id, name, uin
        """
        objects = self.get_catalog_objects(oks_status_array_name=oks_status_array_name)

        result = []
        for obj in objects:
            if "id" in obj and "name" in obj and "uin" in obj:
                result.append({"id": obj["id"], "name": obj["name"], "uin": obj["uin"]})

        return result

    def get_catalog_ids(self, oks_status_array_name=None) -> List[int]:
        """
        Получить только ID всех объектов из каталога
        Возвращает список: [1257, 1258, 1259, ...]
        """
        objects = self.get_catalog_objects(oks_status_array_name=oks_status_array_name)
        return [obj["id"] for obj in objects if "id" in obj]

    def get_dashboard_data(self, object_id) -> bool:
        """
        Проверить наличие видео-ссылок для объекта

        Args:
            object_id (int): ID объекта

        Returns:
            bool: True если есть video_links, False если массив пустой или отсутствует
        """
        response = self.get(f"/api/dashboard/{object_id}")

        if response.status_code == 200:
            try:
                data = response.json()
                video_links = data.get("object", {}).get("video_links")

                if video_links and len(video_links) > 0:
                    return True
                else:
                    return False

            except Exception as e:
                print(f"Ошибка при получении данных: {e}")
                return False

        return False

    def get_catalog_with_video_status(self, oks_status_array_name=None) -> pd.DataFrame:
        """
        Получить датафрейм с объектами и статусом наличия видео

        Args:
            oks_status_array_name (str, optional): Значение для параметра oksStatusArrayName

        Returns:
            pd.DataFrame: Датафрейм с колонками name, id, uin, video
        """
        # Получаем список объектов
        objects_info = self.get_catalog_objects_info(
            oks_status_array_name=oks_status_array_name
        )

        data_for_df = []

        for obj in objects_info:
            # Проверяем наличие видео
            print(f"Обработка объекта {obj['id']}")
            has_video = self.get_dashboard_data(obj["id"])

            data_for_df.append(
                {
                    "name": obj["name"],
                    "id": obj["id"],
                    "uin": obj["uin"],
                    "video": "видео есть" if has_video else "видео нет",
                }
            )
            print(f"Объект {obj['id']} обработан")
            print("*" * 10)

        df = pd.DataFrame(data_for_df)

        return df


if __name__ == "__main__":
    sudir_login = input("Введите логин СУДИР:\n")
    sudir_password = getpass.getpass("Введите пароль СУДИР:\n")

    dashboard_client = DashboardstroiClient(sudir_login, sudir_password)

    try:
        dashboard_client.authorize()
    except Exception as e:
        print(e)
    try:
        print("Авторизация СУДИР пройдена")
        ids = dashboard_client.get_catalog_ids(oks_status_array_name="СМР")
        print(f"Найдено объектов: {len(ids)}")
        print("*" * 10)
        result = dashboard_client.get_catalog_with_video_status(
            oks_status_array_name="СМР"
        )
        print("Объекты обработаны")
        result.to_excel(f"videos_{datetime.now().date()}.xlsx", index=False)
    except Exception as e:
        print(e)
    input()
