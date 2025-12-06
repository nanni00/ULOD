from typing import Optional
from .socrata import SocrataClient


class ChicagoSocrata(SocrataClient):
    def __init__(
        self,
        app_token: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 20,
    ) -> None:
        super().__init__("data.cityofchicago.org", app_token, user, password, timeout)
