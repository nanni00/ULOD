from typing import Optional

from ulod.ckan.ckan import CKAN


class ModenaCKAN(CKAN):
    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://opendata.comune.modena.it/", "/api/3/action", headers)
