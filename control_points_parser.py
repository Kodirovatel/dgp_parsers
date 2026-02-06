import pandas as pd
import requests
from urllib.parse import urlparse, parse_qs, unquote, unquote_plus
from datetime import datetime
import getpass
import time
import numpy as np
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
    
    def get_manager_name(self, object_id) -> str:
        """Получить имя руководителя проекта
        Нужен, т.к. его нет в запросе, который выполняет get_catalog_objects
        """
        response = self.get(f"/api/dashboard/{object_id}")
        if response.status_code == 200:
            try:
                data = response.json()
                return data.get("object", {}).get("project_manager", {}).get('name', 'Нет данных')
            except Exception as e:
                print("Ошибка", e)
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

def collect_etapi_data(client, objects) -> List[Dict[str, Any]]:
    """
    Получаем контрольные точки для объектов
    """

    developer_map = {obj["id"]: obj.get("developer", {}).get("name") for obj in objects}
    planned_commissioning_directive_date_map = {obj['id']: obj.get('planned_commissioning_directive_date') for obj in objects}
    all_data = []

    cash = {}

    for idx, obj in enumerate(objects):
        object_id = obj["id"]
        if object_id not in cash.keys():
            cash[object_id] = client.get_manager_name(object_id)
        object_name = obj["name"]
        developer_name = developer_map.get(object_id)
        planned_commissioning_directive_date = planned_commissioning_directive_date_map.get(object_id)

        print(f"[{idx + 1}/{len(objects)}] Обработка объекта {object_id}...")

        # Получаем данные этапов
        max_retries = 3
        etapi = None
        for attempt in range(max_retries):
            etapi = client.get_etapi_data(object_id)
            if etapi is not None:
                break

            wait_time = (attempt + 1) * 2
            print(f"  Retry {attempt + 1}/{max_retries}, wait {wait_time}s...")
            time.sleep(wait_time)

        if not etapi:
            continue

        control_points = etapi.get("control_points", {})
        in_progress_data = control_points.get("in_progress", {}).get("data", [])
        complete_data = control_points.get("complete", {}).get("data", [])

        # Если нет контрольных точек
        if not in_progress_data and not complete_data:
            print(f"  {object_id} добавлен (без контрольных точек)")

            row = {
                "object_id": object_id,
                "object_name": object_name,
                "status": "no_control_points",
                "developer": developer_name,
                "project_manager": cash[object_id],
                "name": None,
                "plan_finish_date": None,
                "fact_finish_date": None,
                "plan": None,
                "fact": None,
                "created_at": None,
                "updated_at": None,
                "deleted_at": None,
                "planned_commissioning_directive_date": planned_commissioning_directive_date,
                "plan_start_date": None,
                "fact_start_date": None,
                "plan_progress": None,
                "fact_progress": None,
                "readiness": None,
                "color": None,
                "today": datetime.now().date(),
                "suid_url": None,
            }
            all_data.append(row)
            time.sleep(0.7)
            continue

        suid_url = client.get_dashboard_data(object_id)
        print(f"  suid_url: {suid_url}")

        # Обрабатываем контрольные точки
        for status, data in [
            ("in_progress", in_progress_data),
            ("complete", complete_data),
        ]:
            for point in data:
                row = {
                    "object_id": object_id,
                    "object_name": object_name,
                    "status": status,
                    "name": point.get("name"),
                    "developer": developer_name,
                    "project_manager": cash[object_id],
                    "plan_finish_date": point.get("plan_finish_date"),
                    "fact_finish_date": point.get("fact_finish_date"),
                    "plan": point.get("plan"),
                    "fact": point.get("fact"),
                    "created_at": point.get("created_at"),
                    "updated_at": point.get("updated_at"),
                    "deleted_at": point.get("deleted_at"),
                    "planned_commissioning_directive_date": planned_commissioning_directive_date,
                    "plan_start_date": point.get("plan_start_date"),
                    "fact_start_date": point.get("fact_start_date"),
                    "plan_progress": point.get("plan_progress"),
                    "fact_progress": point.get("fact_progress"),
                    "readiness": point.get("readiness"),
                    "color": point.get("color"),
                    "today": datetime.now().date(),
                    "suid_url": suid_url,
                }
                all_data.append(row)

        print(f"  {object_id} готов")
        time.sleep(0.7)

    return all_data


class SUIDClient:
    """Клиент для работы с suid.mos.ru"""

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.token = None
        self.session = requests.Session()

    def authorize(self) -> str:
        """Авторизация и получение токена"""
        resp = requests.post(
            "https://suid.mos.ru/auth/realms/SpringBoot/protocol/openid-connect/token",
            data={
                "grant_type": "password",
                "client_id": "ExonReactApp",
                "username": self.username,
                "password": self.password,
                "scope": "openid",
            },
            timeout=10,
            verify=False,
        )

        if not resp.ok:
            raise Exception(f"Ошибка авторизации SUID: {resp.status_code}")

        self.token = resp.json()["access_token"]
        return self.token

    def get(self, url) -> requests.Response:
        """GET запрос с токеном"""
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        response = self.session.get(url, headers=headers, verify=False)
        return response

    def post(self, url, json_data=None) -> requests.Response:
        """POST запрос с токеном"""
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        response = self.session.post(url, json=json_data, headers=headers, verify=False)
        return response

    def get_works(self, suid_url) -> List[Dict[str, Any]]:
        """
        Получить список работ из SUID
        Возвращает список словарей [{end_date: ..., name: ...}, ...]
        """
        # POST на /all
        uuid = suid_url.rstrip("/").split("/")[-1]

        # Строим правильный API URL
        api_url = f"https://suid.mos.ru/api/isr-new-service/common/{uuid}/all"

        print(f"  POST URL: {api_url}")

        response = self.post(api_url)

        print(f"  Status: {response.status_code}")

        if response.status_code != 200:
            print(f"  Response: {response.text[:200]}")
            return []

        try:
            data = response.json()
            works = data.get("works", [])

            result = []
            for work in works:
                result.append(
                    {
                        "start_date": work.get("start_date"),
                        "end_date": work.get("end_date"),
                        "name": work.get("name"),
                        "fact_end_date": work.get("fact_end_date"),
                    }
                )

            return result
        except Exception as e:
            print(f"  Ошибка парсинга: {e}")
            return []


def mark_column_as(
    objects: str,
    objects_column: str,
    objects_column_in_dataframe: str,
    dataframe: pd.DataFrame,
    column: str,
    value: Any,
):
    """
    Берет объекты из файла и помечает их в датафрейме указанным значением в указанной колонке

    :param objects (str): название файла
    :param objects_column (str): название колонки где лежат объекты в object_list
    :param objects_column_in_dataframe (str): название колонки где лежат объекты в датафрейме
    :param dataframe (pd.DataFrame): датафрейм с данными для изменения
    :param column (str): в какой колонке делать отмеку
    :param value (Any): какое значение выставить
    """

    try:
        objects_list = list(pd.read_excel(objects)[objects_column])
    except FileNotFoundError:
        print(f"Файл {objects} не найден")
        return
    except Exception as e:
        print(f"Ошибка с файлом {objects}: {e}")
        return
    mask = dataframe[objects_column_in_dataframe].isin(objects_list)
    dataframe.loc[mask, column] = value
    print(f"Помечено строк: {mask.sum()}")


# ============= ИСПОЛЬЗОВАНИЕ =============

if __name__ == "__main__":
    print(
        "Для проверки дополнительных котрольных точек положить их названия в control_points.xlsx"
    )
    print(
        "Чтобы отметить объекты как исключенные из проверки, надо поместить их id в файл excluded_objects.xlsx"
    )
    # Создаём клиент и авторизуемся в судир и суид
    sudir_login = input("Введите логин СУДИР:\n")
    sudir_password = getpass.getpass("Введите пароль СУДИР:\n")
    suid_login = input("Введите логин СУИД: ")
    suid_password = getpass.getpass("Введите пароль СУИД: ")


    dashboard_client = DashboardstroiClient(sudir_login, sudir_password)
    suid_client = SUIDClient(suid_login, suid_password)
    try:
        dashboard_client.authorize()
        print("Авторизация СУДИР пройдена")
        suid_client.authorize()
        print("Логин и пароль для СУИД верны")
        ids = dashboard_client.get_catalog_ids()
        print(f"Найдено объектов: {len(ids)}")

        objects = dashboard_client.get_catalog_objects()

        # Собираем контрольные точки из дашборда
        result = collect_etapi_data(dashboard_client, objects)
        df = pd.DataFrame(result)
        df_new = df.copy()
        df_new.loc[:, "plan_finish_date"] = pd.to_datetime(
            df_new["plan_finish_date"], format="%Y-%m-%d", errors="coerce"
        ).dt.date
        df_new.loc[:, "fact_finish_date"] = pd.to_datetime(
            df_new["fact_finish_date"], format="%Y-%m-%d", errors="coerce"
        ).dt.date
        df_new.loc[:, "planned_commissioning_directive_date"] = pd.to_datetime(
            df_new["planned_commissioning_directive_date"], format="%d.%m.%Y", errors="coerce"
        ).dt.date
        df_new.loc[:, "is_exceeded"] = np.where(
            df_new["status"] == "no_control_points",
            True,  # Если нет контрольных точек
            np.where(
                df_new["plan_finish_date"] == df_new["fact_finish_date"],
                False,  # Если план = факт
                df_new["plan_finish_date"]
                <= df_new["today"],  # Иначе проверяем просрочку
            ),
        )

        df_new.loc[:, "etapi_url"] = "https://dashboard-stroi.mos.ru/etapi/" + df_new[
            "object_id"
        ].astype(str)
        df_new.loc[:, "dasboard_url"] = "https://dashboard-stroi.mos.ru/dashboard/" + df_new[
            "object_id"
        ].astype(str)
        df_new.loc[:, "control_point_comparsion"] = df_new["suid_url"].apply(
            lambda x: "объект exon" if pd.notna(x) and "exonproject" in str(x) else None
        )
        # Идём в SUID
        to_process = df_new[
            (df_new["control_point_comparsion"].isna())
            & (df_new["status"] != "no_control_points")
            & (df_new["is_exceeded"] == True)
        ].copy()
        if len(to_process) > 0:
            print("\n=== Обновление токена SUID ===")
            suid_client = SUIDClient(suid_login, suid_password)
            suid_client.authorize()
            print("Авторизация в СУИД успешна")
            print()

            print("Получение данных из SUID")

            # Получаем уникальные object_id
            unique_objects = to_process[["object_id", "suid_url"]].drop_duplicates(
                "object_id"
            )

            # Кеш для работ из SUID по object_id
            suid_works_cache = {}

            for idx, (_, row) in enumerate(unique_objects.iterrows(), 1):
                object_id = row["object_id"]
                suid_url = row["suid_url"]

                print(
                    f"[{idx}/{len(unique_objects)}] Получение работ для объекта {object_id}..."
                )

                works = suid_client.get_works(suid_url)

                # Cловарь {name.lower(): end_date} для быстрого поиска
                works_dict = {}
                for work in works:
                    name_lower = work["name"].lower()
                    works_dict[name_lower] = {
                        "end_date": work["end_date"],
                        "start_date": work["start_date"],
                        "fact_end_date_suid": work["fact_end_date"],
                    }
                suid_works_cache[object_id] = works_dict

                print(f"  Найдено работ: {len(works)}")
                time.sleep(0.5)

            print("\n=== Сопоставление данных ===")

            df_new["suid_end"] = None
            df_new["suid_start"] = None
            df_new["fact_end_date_suid"] = None

            # Заполняем suid_end для отфильтрованных строк
            for idx in to_process.index:
                object_id = df_new.loc[idx, "object_id"]
                name = df_new.loc[idx, "name"]

                if object_id in suid_works_cache and name:
                    name_lower = name.lower()
                    works_dict = suid_works_cache[object_id]

                    if name_lower in works_dict:
                        df_new.loc[idx, "suid_start"] = works_dict[name_lower][
                            "start_date"
                        ]
                        df_new.loc[idx, "suid_end"] = works_dict[name_lower]["end_date"]
                        df_new.loc[idx, "fact_end_date_suid"] = works_dict[name_lower][
                            "fact_end_date_suid"
                        ]
                        print(f"  Сопоставлено: {object_id} - {name[:50]}...")
                    else:
                        df_new.loc[idx, "control_point_comparsion"] = (
                            "РАБОТА НЕ НАЙДЕНА"
                        )
                        print(f"  Не найдено: {object_id} - {name[:50]}...")

            matched = df_new["suid_end"].notna().sum()
            print(f"\nИтого сопоставлено: {matched} из {len(to_process)}")

            # Конвертируем suid_end в date
            df_new["suid_end"] = pd.to_datetime(
                df_new["suid_end"], format="%Y-%m-%d", errors="coerce"
            ).dt.date
            df_new["suid_start"] = pd.to_datetime(
                df_new["suid_start"], format="%Y-%m-%d", errors="coerce"
            ).dt.date

            # Сравниваем даты
            df_new["is_dates_equal"] = df_new["plan_finish_date"] == df_new["suid_end"]

            print(f"Дат совпадает: {df_new['is_dates_equal'].sum()}")

        # Дополнительные контрольные точки из файла
        try:
            control_points_df = pd.read_excel("control_points.xlsx")
            control_points_list = (
                control_points_df["points"].dropna().str.strip().str.lower().tolist()
            )

            print(f"Загружено контрольных точек: {len(control_points_list)}")
            for i, cp in enumerate(control_points_list, 1):
                print(f"  {i}. {cp}")
        except FileNotFoundError:
            print(
                "Файл 'control_points.xlsx' не найден, пропускаем дополнительные точки"
            )
            control_points_list = []

        if control_points_list:
            # Переавторизация в SUID для свежего токена
            print("\n=== Обновление токена SUID для дополнительных точек ===")
            suid_client = SUIDClient(suid_login, suid_password)
            suid_client.authorize()

            # Получаем уникальные объекты с suid_url (исключая exon)
            objects_for_additional = (
                df_new[
                    (df_new["suid_url"].notna())
                    & (~df_new["suid_url"].str.contains("exonproject", na=False))
                ][["object_id", "object_name", "suid_url"]]
                .drop_duplicates("object_id")
                .reset_index(drop=True)
            )

            print(f"Объектов для проверки: {len(objects_for_additional)}")

            additional_data = []

            for idx, row in objects_for_additional.iterrows():
                object_id = row["object_id"]
                object_name = row["object_name"]
                suid_url = row["suid_url"]

                print(
                    f"[{idx + 1}/{len(objects_for_additional)}] Объект {object_id}..."
                )

                # Получаем работы
                works = suid_client.get_works(suid_url)
                works_dict = {work["name"].lower(): work for work in works}

                # Проверяем каждую точку из списка
                for cp_name in control_points_list:
                    if cp_name in works_dict:
                        work = works_dict[cp_name]
                        additional_data.append(
                            {
                                "object_id": object_id,
                                "object_name": object_name,
                                "status": "additional_check",
                                "name": cp_name,
                                "project_manager": df_new[df_new["object_id"] == object_id]["project_manager"].iloc[0],
                                "developer": df_new[df_new["object_id"] == object_id][
                                    "developer"
                                ].iloc[0],
                                "plan_finish_date": None,
                                "fact_finish_date": None,
                                "suid_start": work.get("start_date"),
                                "suid_end": work.get("end_date"),
                                "fact_end_date_suid": work.get("fact_end_date"),
                                "found_in_suid": True,
                                "today": datetime.now().date(),
                                "suid_url": suid_url,
                                "control_point_comparsion": "дополнительная точка",
                                "etapi_url": f"https://dashboard-stroi.mos.ru/etapi/{object_id}",
                                "dashboard_url": f"https://dashboard-stroi.mos.ru/dashboard/{object_id}",
                            }
                        )
                        print(f"  Есть {cp_name}")
                    else:
                        additional_data.append(
                            {
                                "object_id": object_id,
                                "object_name": object_name,
                                "status": "additional_check",
                                "name": cp_name,
                                "project_manager": df_new[df_new["object_id"] == object_id]["project_manager"].iloc[0],
                                "developer": df_new[df_new["object_id"] == object_id][
                                    "developer"
                                ].iloc[0],
                                "plan_finish_date": None,
                                "fact_finish_date": None,
                                "suid_start": None,
                                "suid_end": None,
                                "fact_end_date_suid": work.get("fact_end_date"),
                                "found_in_suid": False,
                                "today": datetime.now().date(),
                                "suid_url": suid_url,
                                "control_point_comparsion": "не найдена в СУИД",
                                "etapi_url": f"https://dashboard-stroi.mos.ru/etapi/{object_id}",
                                "dashboard_url": f"https://dashboard-stroi.mos.ru/dashboard/{object_id}",
                            }
                        )
                        print(f"  Нет {cp_name}")

                time.sleep(0.5)

            # Добавляем дополнительные данные к основному датафрейму
            if additional_data:
                df_additional = pd.DataFrame(additional_data)
                df_additional["suid_start"] = pd.to_datetime(
                    df_additional["suid_start"], format="%Y-%m-%d", errors="coerce"
                ).dt.date
                df_additional["suid_end"] = pd.to_datetime(
                    df_additional["suid_end"], format="%Y-%m-%d", errors="coerce"
                ).dt.date

                # Объединяем
                df_new = pd.concat([df_new, df_additional], ignore_index=True)
                print(f"\nДобавлено дополнительных записей: {len(additional_data)}")

        if "found_in_suid" in df_new.columns:
            df_new["found_in_suid"] = df_new["found_in_suid"].replace(
                {1.0: True, 0.0: False}
            )
        df_new["is_fact_date_suid_is_equal"] = (
            df_new["plan_finish_date"] == df_new["fact_end_date_suid"]
        )
        df_new['is_directive_equal'] = (
            df['plan_finish_date'] == df['planned_commissioning_directive_date']
        )

        print("Обращаемся к файлу excluded_objects.xlsx")
        mark_column_as(
            objects="excluded_objects.xlsx",
            objects_column="id",
            objects_column_in_dataframe="object_id",
            dataframe=df_new,
            column="exclude_from_check",
            value=True,
        )
        df_new.to_excel(f"control_points_{datetime.now().date()}.xlsx", index=False)
        input("Готово!")
    except Exception as e:
        print(f"Ошибка: {e}")