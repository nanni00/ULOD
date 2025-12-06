from .ckan import CKAN


class UKCKAN(CKAN):
    def __init__(self, headers: dict) -> None:
        super().__init__("https://data.gov.uk", "/api/action", headers)
