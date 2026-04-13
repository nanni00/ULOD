from typing import Optional

from ulod.sources import CKAN


class UK(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://data.gov.uk", "/api/action", headers, connection_kw)


class NHSUK(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__(
            "https://opendata.nhsbsa.net/", "/api/3/action", headers, connection_kw
        )
