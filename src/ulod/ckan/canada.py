from typing import Optional

from ulod.ckan.ckan import CKAN


class CanadaCKAN(CKAN):
    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://open.canada.ca", "/data/api/3/action", headers)
