from typing import Optional

from ulod.sources import CKAN, ODS


class Italy(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://dati.gov.it", "/opendata/api/3/action", headers)


class Modena(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://opendata.comune.modena.it/", "/api/3/action", headers)


class Ferrara(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://dati.comune.fe.it/", "/api/3/action", headers)


class Milano(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://dati.comune.milano.it/", "/api/3/action", headers)


class Bologna(ODS):
    source_type = "ods"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__(
            "https://opendata.comune.bologna.it", "/api/explore/v2.1", headers
        )
