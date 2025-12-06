from .ckan import CKAN


class ItalyCKAN(CKAN):
    def __init__(self, headers: dict) -> None:
        super().__init__("https://dati.gov.it", "/opendata/api/3/action", headers)
