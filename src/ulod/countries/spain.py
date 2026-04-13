from typing import Optional

from ulod.sources import CKAN


class Madrid(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://datos.madrid.es/", "/api/3/action", headers)
