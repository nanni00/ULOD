from .ckan import CKAN


class CanadaCKAN(CKAN):
    def __init__(self, headers: dict) -> None:
        super().__init__("https://open.canada.ca", "/data/api/3/action", headers)
