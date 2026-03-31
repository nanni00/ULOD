from typing import Optional

from ulod.ckan.ckan import CKAN


class MadridCKAN(CKAN):
    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://datos.madrid.es/", "/api/3/action", headers)
