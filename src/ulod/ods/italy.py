from ulod.ods.ods import ODS

from typing import Optional


class BolognaODS(ODS):
    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__(
            "https://opendata.comune.bologna.it", "/api/explore/v2.1", headers
        )
