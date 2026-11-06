"""CKAN catalog operations delegated to ckanapi."""

from urllib.parse import urljoin

import requests
from ckanapi import RemoteCKAN

from .common import Dataset, Resource, infer_format, paged_entries, resource_id


class CKANAdapter:
    def __init__(self, url, cfg, http):
        self.url, self.cfg, self.http = url, cfg, http
        session = requests.Session()
        session.trust_env = False
        session.headers.update(http.headers)
        self.client = RemoteCKAN(
            url, apikey=cfg.api_key, get_only=True, session=session
        )

    def action(self, action, **data):
        return self.http.retry(
            lambda: self.client.call_action(
                action,
                data,
                requests_kwargs={"timeout": self.cfg.timeout, "allow_redirects": False},
            )
        )

    def page(self, offset):
        page = self.action(
            "package_search", start=offset, rows=self.cfg.page_size, sort="id asc"
        )
        return paged_entries(page, "results", "count", offset)

    @staticmethod
    def identifier(entry):
        return str(entry["id"])

    def describe(self, entry):
        metadata = self.action("package_show", id=self.identifier(entry))
        resources = []
        for raw in metadata.get("resources", []):
            url = raw.get("url")
            url = urljoin(self.url + "/", url) if url else None
            fmt = infer_format(url or "", raw.get("format") or raw.get("mimetype"))
            reason = None
            if fmt in {"api", "wms", "wfs", "wcs", "wmts", "arcgis"}:
                reason = f"Service resource ({fmt}) has no supported file export"
            elif not url:
                if raw.get("datastore_active"):
                    url = f"{self.url}/datastore/dump/{raw['id']}?format=csv"
                    fmt = "csv"
                else:
                    reason = "Resource has no download URL"
            resources.append(Resource(resource_id(raw), url, fmt, raw, reason=reason))
        if not resources:
            resources.append(
                Resource("data", None, reason="Package has no downloadable resources")
            )
        return Dataset(self.identifier(entry), metadata, resources)

    def close(self):
        self.client.close()
