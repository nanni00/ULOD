from typing import Optional

from ulod.sources import ODS


class Paris(ODS):
    source_type = "ods"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://opendata.paris.fr", "/api/explore/v2.1", headers)
