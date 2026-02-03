from typing import Optional

from ulod.ckan.ckan import CKAN


class ItalyCKAN(CKAN):
    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://dati.gov.it", "/opendata/api/3/action", headers)


class ModenaCKAN(CKAN):
    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://opendata.comune.modena.it/", "/api/3/action", headers)


class FerraraCKAN(CKAN):
    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://dati.comune.fe.it/", "/api/3/action", headers)
