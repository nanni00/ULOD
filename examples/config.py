from pathlib import Path

from fake_useragent import UserAgent


_ua = UserAgent()
headers = {"User-Agent": _ua.firefox}
connection_pool_kw = {"redirect": True, "timeout": 5}

ROOT_DATA_PATH = Path.home() / "data"
ULOD_DATA_PATH = ROOT_DATA_PATH / "ulod"
ODS_DATA_PATH = ULOD_DATA_PATH / "ods"
CKAN_DATA_PATH = ULOD_DATA_PATH / "ckan"
SOCRATA_DATA_PATH = ULOD_DATA_PATH / "socrata"