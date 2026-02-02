import pandas as pd
import requests
import getpass
from urllib.parse import urlparse, parse_qs, unquote, unquote_plus
from typing import Optional, Any, List
import hashlib
import time
from datetime import datetime, timedelta


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
                data = response.json()
                return data.get("object", {}).get("suid_ksg_url")
            except Exception as e:
                print("Ссылки на суид нет", e)
                return None
        return None

    def get_etapi_data(self, object_id) -> Optional[Any]:
        """Получить данные этапов для объекта"""
        start = time.time()
        response = self.get(f"/api/etapi/{object_id}")
        elapsed = time.time() - start
        print(f"  Request time: {elapsed:.2f}s")
        print(f"Object {object_id}: status={response.status_code}")

        if response.status_code != 200:
            print(f"  ERROR: {response.text[:200]}")
            return None

        try:
            data = response.json()
            return data
        except Exception as e:
            print(f"  JSON parse error: {e}")
            print(f"  Response: {response.text[:200]}")
            return None

    def get_photo_status(self, object_id) -> list:
        """
        Проверить статус фотографий объекта
        Возвращает: ['нет фотографий' / 'есть совпадение' / 'фото в порядке', есть ли фото за вчера]
        """
        response = self.get(f"/api/dashboard/{object_id}")

        if response.status_code != 200:
            return ["нет фотографий", False]

        try:
            data = response.json()
            photos = data.get("photos", [])

            if not photos:
                return ["нет фотографий", False]

            # Получаем basePhotoUrl и uin
            base_photo_url = data.get("basePhotoUrl", "")
            object_uin = data.get("object", {}).get("uin", "")

            # Извлекаем все уникальные даты
            dates = set()
            for photo in photos:
                taken_at = photo.get("taken_at")
                if taken_at:
                    date_only = taken_at.split()[0] if " " in taken_at else taken_at
                    dates.add(date_only)

            sorted_dates = sorted(dates, reverse=True)
            last_two_dates = sorted_dates[:2]
            yesterday_str = (datetime.now().date() - timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )
            yesterday = yesterday_str in last_two_dates

            # Если дата только одна
            if len(last_two_dates) < 2:
                return ["есть совпадение", yesterday]

            # Если даты разные, проверяем фотки на дубликаты
            if last_two_dates[0] != last_two_dates[1]:
                # Получаем фотки для двух последних дат
                photos_date1 = [
                    p
                    for p in photos
                    if p.get("taken_at", "").startswith(last_two_dates[0])
                ]
                photos_date2 = [
                    p
                    for p in photos
                    if p.get("taken_at", "").startswith(last_two_dates[1])
                ]

                # Вычисляем хеши для фоток первой даты
                hashes_date1 = set()
                for photo in photos_date1:
                    photo_hash = self.get_photo_hash(
                        photo.get("photo_url"),
                        base_photo_url,
                        object_uin,
                        last_two_dates[0],
                    )
                    if photo_hash:
                        hashes_date1.add(photo_hash)

                # Проверяем на совпадения со второй датой
                for photo in photos_date2:
                    photo_hash = self.get_photo_hash(
                        photo.get("photo_url"),
                        base_photo_url,
                        object_uin,
                        last_two_dates[1],
                    )
                    if photo_hash and photo_hash in hashes_date1:
                        return ["есть совпадение", yesterday]

                return ["фото в порядке", yesterday]

            return ["есть совпадение", yesterday]

        except Exception as e:
            print(f"  Ошибка получения фото для {object_id}: {e}")
            return ["нет фотографий", yesterday]

    def get_photo_hash(
        self, photo_url, base_photo_url, object_uin, date
    ) -> Optional[str]:
        """Получить MD5 хеш фотографии"""
        if not photo_url or not base_photo_url or not object_uin or not date:
            return None

        try:
            # Строим правильный URL: basePhotoUrl + uin + '/' + дата + '/' + photo_url
            full_url = f"{base_photo_url}{object_uin}/{date}/{photo_url}"

            response = self.session.get(full_url, timeout=5)

            if response.status_code == 200:
                # Вычисляем MD5 хеш содержимого
                return hashlib.md5(response.content).hexdigest()
        except Exception as e:
            print(f"  Ошибка загрузки фото {photo_url}: {e}")

        return None

if __name__ == "__main__":
    sudir_login = input("Введите логин СУДИР:\n")
    sudir_password = getpass.getpass("Введите пароль СУДИР:\n")
    dashboard_client = DashboardstroiClient(sudir_login, sudir_password)
    try:
        dashboard_client.authorize()
        print("Авторизация СУДИР пройдена")
        objects = dashboard_client.get_catalog_objects()
        df_new = pd.DataFrame(objects).rename(columns={'id': 'object_id'})
        print("\n=== Проверка фотографий объектов ===")
        print(
            "Файл для проверки объектов на наличие вчерашних фотографий yesterday_photo_objects.xlsx"
        )
        try:
            yesterday_photos_df = pd.read_excel("yesterday_photo_objects.xlsx")
            yesterday_photos_list = yesterday_photos_df["id"].dropna().tolist()
            print("     Данные из yesterday_photo_objects.xlsx получены")
        except Exception as e:
            print(f"Не удалось получить данные из yesterday_photo_objects.xlsx, {e}")
            yesterday_photos_list = []

        df_new = df_new[df_new["object_id"].isin(yesterday_photos_list)]
        unique_objects = df_new.drop_duplicates(subset=["object_id"])
        photo_status_cache = {}
        for idx, row in unique_objects.iterrows():
            object_id = row["object_id"]
            print(f"Проверка фото для объекта {object_id}...")

            status = dashboard_client.get_photo_status(object_id)
            photo_status_cache[object_id] = [status[0], status[1]]

            print(f"  Статус: {status[0]}")
            time.sleep(0.5)

        df_new["photo_status"] = df_new["object_id"].map(
            lambda oid: photo_status_cache[oid][0]
        )
        df_new["yesterday_photo"] = df_new["object_id"].map(
            lambda oid: photo_status_cache[oid][1]
        )

        print("\nСтатистика фотографий:")
        print(df_new["photo_status"].value_counts())
        df_new[['object_id', 'uin', 'name', 'address', 'photo_status', 'yesterday_photo']].to_excel('test.xlsx', index=False, )
    except Exception as e:
        print(f'ошибка {e}')