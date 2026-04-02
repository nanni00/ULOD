from ulod.ods.ods import ODS

from typing import Optional


class ParisODS(ODS):
    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://opendata.paris.fr", "/api/explore/v2.1", headers)
