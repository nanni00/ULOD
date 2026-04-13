from typing import Optional

from ulod.sources import SocrataClient


class Chicago(SocrataClient):
    source_type = "socrata"

    def __init__(
        self,
        app_token: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 20,
    ) -> None:
        super().__init__("data.cityofchicago.org", app_token, user, password, timeout)


class NYC(SocrataClient):
    source_type = "socrata"

    def __init__(
        self,
        app_token: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 20,
    ) -> None:
        super().__init__("data.cityofnewyork.us", app_token, user, password, timeout)
