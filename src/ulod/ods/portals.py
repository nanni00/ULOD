from typing import Optional

from ulod.ods.client import ODS

__all__ = ["Paris", "Bologna"]


class Paris(ODS):
    source_type = "ods"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://opendata.paris.fr", "/api/explore/v2.1", headers)


class Bologna(ODS):
    source_type = "ods"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__(
            "https://opendata.comune.bologna.it", "/api/explore/v2.1", headers
        )
