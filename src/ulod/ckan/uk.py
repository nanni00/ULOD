from typing import Optional

from ulod.ckan.ckan import CKAN


class UKCKAN(CKAN):
    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://data.gov.uk", "/api/action", headers, connection_kw)
