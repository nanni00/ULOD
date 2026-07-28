from __future__ import annotations

from typing import Any, Optional

from ulod.ckan.client import CKAN, SessionCKAN

__all__ = [
    "Canada",
    "Italy",
    "Modena",
    "Ferrara",
    "Milano",
    "Madrid",
    "Valencia",
    "UK",
    "NHSUK",
]


class Canada(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://open.canada.ca", "/data/api/3/action", headers)


class Italy(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://dati.gov.it", "/opendata/api/3/action", headers)


class Modena(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://opendata.comune.modena.it/", "/api/3/action", headers)


class Ferrara(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://dati.comune.fe.it/", "/api/3/action", headers)


class Milano(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://dati.comune.milano.it/", "/api/3/action", headers)


class Madrid(SessionCKAN):
    source_type = "ckan"

    _default_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        merged_headers = self._default_headers | headers
        super().__init__(
            "https://datos.madrid.es/",
            "/api/3/action",
            merged_headers,
            connection_kw,
        )

    def default_bulk_download_policy(self) -> dict[str, Any]:
        return {
            "request_delay_s": 0.75,
            "request_jitter_s": 0.25,
            "retry_backoff_base_s": 2.0,
            "cooldown_on_403_s": 15.0,
            "max_consecutive_403": 3,
            "session_warmup_url": f"{self.base_url}/",
        }


class Valencia(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__(
            "https://opendata.vlci.valencia.es",
            "/api/3/action/",
            headers,
            connection_kw,
        )


class UK(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__("https://data.gov.uk", "/api/action", headers, connection_kw)


class NHSUK(CKAN):
    source_type = "ckan"

    def __init__(self, headers: dict, connection_kw: Optional[dict] = None) -> None:
        super().__init__(
            "https://opendata.nhsbsa.net/", "/api/3/action", headers, connection_kw
        )
