from .ckan import CKAN


class ModenaCKAN(CKAN):
    def __init__(self, headers: dict) -> None:
        super().__init__("https://opendata.comune.modena.it/", "/api/3/action", headers)
