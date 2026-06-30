from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Literal
from urllib.parse import urlencode

import urllib3

from ulod.sources import Source
from ulod.sources.ckan import StreamResponse
from ulod.utils.exceptions import HTTPResourceError

__all__ = ["US"]

Sort = Literal["relevance", "popularity", "distance", "last_harvested_date"]
SpatialFilter = Literal["geospatial", "non-geospatial"]
OrgType = Literal[
    "Federal Government",
    "City Government",
    "State Government",
    "County Government",
    "University",
    "Tribal",
    "Non-Profit",
]


class US(Source):
    source_type = "datagov"

    def __init__(
        self,
        base_url: str = "https://catalog.data.gov",
        headers: Mapping[str, str] | None = None,
        connection_kw: Mapping[str, Any] | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.headers = dict(headers or {})
        self.connection_kw = dict(connection_kw or {})

    def request_json(
        self, path: str, params: Mapping[str, Any] | None = None
    ) -> dict[str, Any]:
        url = self._url(path, params or {})
        response = urllib3.request(
            "GET", url, headers=self.headers, **self.connection_kw
        )
        if response.status >= 400:
            raise HTTPResourceError(url, response.status)
        return response.json()

    def stream_request(self, url: str) -> StreamResponse:
        response = urllib3.request(
            "GET",
            url,
            preload_content=False,
            decode_content=False,
            headers=self.headers,
            **self.connection_kw,
        )
        return StreamResponse(
            status=response.status,
            headers=response.headers,
            _iter_content=response.stream,
            _close=response.release_conn,
        )

    def search(
        self,
        q: str = "",
        sort: Sort = "relevance",
        per_page: int = 10,
        after: str | None = None,
        org_slug: str | None = None,
        org_type: OrgType | None = None,
        keyword: str | Sequence[str] | None = None,
        spatial_filter: SpatialFilter | None = None,
        spatial_geometry: str | Mapping[str, Any] | None = None,
        spatial_within: bool | None = None,
    ) -> dict[str, Any]:
        return self.request_json(
            "/search",
            {
                "q": q,
                "sort": sort,
                "per_page": per_page,
                "after": after,
                "org_slug": org_slug,
                "org_type": org_type,
                "keyword": keyword,
                "spatial_filter": spatial_filter,
                "spatial_geometry": spatial_geometry,
                "spatial_within": spatial_within,
            },
        )

    def get_dataset(self, identifier: str) -> dict[str, Any]:
        for dataset in self.iter_datasets(q=identifier, sort="relevance", per_page=25):
            if self._matches_dataset(dataset, identifier):
                return dataset
        raise LookupError(f"No dataset found for {identifier!r}")

    def get_metadata(self, identifier: str) -> dict[str, Any]:
        dataset = self.get_dataset(identifier)
        dcat = dataset.get("dcat")
        return dict(dcat) if isinstance(dcat, Mapping) else dataset

    def get_keywords(self, size: int = 100, min_count: int = 1) -> dict[str, Any]:
        return self.request_json(
            "/api/keywords", {"size": size, "min_count": min_count}
        )

    def get_organizations(self) -> dict[str, Any]:
        return self.request_json("/api/organizations")

    def iter_metadata_pages(
        self,
        q: str = "",
        sort: Sort = "last_harvested_date",
        per_page: int = 100,
        after: str | None = None,
        org_slug: str | None = None,
        org_type: OrgType | None = None,
        keyword: str | Sequence[str] | None = None,
        spatial_filter: SpatialFilter | None = None,
        spatial_geometry: str | Mapping[str, Any] | None = None,
        spatial_within: bool | None = None,
    ) -> Iterator[dict[str, Any]]:
        cursor = after
        while True:
            page = self.search(
                q=q,
                sort=sort,
                per_page=per_page,
                after=cursor,
                org_slug=org_slug,
                org_type=org_type,
                keyword=keyword,
                spatial_filter=spatial_filter,
                spatial_geometry=spatial_geometry,
                spatial_within=spatial_within,
            )
            yield page

            next_cursor = page.get("after")
            if (
                not isinstance(next_cursor, str)
                or not next_cursor
                or next_cursor == cursor
            ):
                break
            cursor = next_cursor

    def iter_datasets(
        self,
        q: str = "",
        sort: Sort = "last_harvested_date",
        per_page: int = 100,
        after: str | None = None,
        org_slug: str | None = None,
        org_type: OrgType | None = None,
        keyword: str | Sequence[str] | None = None,
        spatial_filter: SpatialFilter | None = None,
        spatial_geometry: str | Mapping[str, Any] | None = None,
        spatial_within: bool | None = None,
    ) -> Iterator[dict[str, Any]]:
        for page in self.iter_metadata_pages(
            q=q,
            sort=sort,
            per_page=per_page,
            after=after,
            org_slug=org_slug,
            org_type=org_type,
            keyword=keyword,
            spatial_filter=spatial_filter,
            spatial_geometry=spatial_geometry,
            spatial_within=spatial_within,
        ):
            yield from page.get("results", [])

    def _url(self, path: str, params: Mapping[str, Any]) -> str:
        endpoint = path if path.startswith("/") else f"/{path}"
        query = urlencode(self._params(params), doseq=True)
        return f"{self.base_url}{endpoint}{f'?{query}' if query else ''}"

    @staticmethod
    def _params(params: Mapping[str, Any]) -> dict[str, Any]:
        clean: dict[str, Any] = {}
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, bool):
                clean[key] = str(value).lower()
            elif isinstance(value, Mapping):
                clean[key] = json.dumps(value, separators=(",", ":"))
            else:
                clean[key] = value
        return clean

    @staticmethod
    def _matches_dataset(dataset: Mapping[str, Any], identifier: str) -> bool:
        if dataset.get("identifier") == identifier or dataset.get("slug") == identifier:
            return True
        dcat = dataset.get("dcat")
        return isinstance(dcat, Mapping) and dcat.get("identifier") == identifier
