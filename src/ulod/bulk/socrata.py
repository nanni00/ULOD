"""Sodapy metadata plus streamed exports, file assets and attachments."""

from urllib.parse import quote, urlencode, urljoin, urlparse

from sodapy import Socrata

from .common import Dataset, Resource, infer_format, paged_entries, resource_id


class SocrataAdapter:
    def __init__(self, url, cfg, http):
        self.url, self.cfg, self.http = url, cfg, http
        self.clients = {}

    def page(self, offset):
        # sodapy.datasets(limit=N) rejects a short final page when total > N.
        # Read the catalog envelope directly to validate counts and checkpoint pages.
        page = self.http.json(
            self.url + "/api/catalog/v1",
            params={
                "domains": urlparse(self.url).netloc,
                "limit": self.cfg.page_size,
                "offset": offset,
            },
        )
        if page.get("warnings"):
            raise ValueError(f"Catalog discovery warnings: {page['warnings']}")
        return paged_entries(page, "results", "resultSetSize", offset)

    @staticmethod
    def identifier(entry):
        return str(entry["resource"]["id"])

    def client(self, domain):
        if domain not in self.clients:
            same_domain = domain == urlparse(self.url).netloc
            client = Socrata(
                domain,
                self.cfg.app_token if same_domain else None,
                access_token=self.cfg.access_token if same_domain else None,
                timeout=self.cfg.timeout,
            )
            client.session.trust_env = False
            if same_domain:
                client.session.headers.update(self.cfg.headers)
                client.uri_prefix = urlparse(self.url).scheme + "://"
            self.clients[domain] = client
        return self.clients[domain]

    def describe(self, entry):
        identifier = self.identifier(entry)
        domain = entry.get("metadata", {}).get("domain") or urlparse(self.url).netloc
        if not domain or any(c in domain for c in "/@?#"):
            raise ValueError("Invalid Socrata asset domain")
        client = self.client(domain)
        metadata = self.http.retry(lambda: client.get_metadata(identifier))
        root = client.uri_prefix + domain
        view = f"{root}/api/views/{quote(identifier, safe='')}"
        dataset = Dataset(identifier, metadata)
        kind = entry["resource"].get("type")
        view_type = metadata.get("viewType")
        if metadata.get("blobId") or view_type == "blobby":
            fmt = infer_format(
                declared=metadata.get("blobMimeType"),
                filename=metadata.get("blobFilename", ""),
            )
            blob = metadata.get("blobId")
            url = (
                f"{view}/files/{quote(str(blob), safe='')}?download=true"
                if blob
                else None
            )
            dataset.resources.append(Resource("file", url, fmt, metadata))
        elif view_type == "geo":
            for fmt, api_format in (
                ("geojson", "GeoJSON"),
                ("zip", "Shapefile"),
                ("kml", "KML"),
                ("kmz", "KMZ"),
            ):
                url = (
                    f"{root}/api/geospatial/{quote(identifier, safe='')}?"
                    + urlencode({"method": "export", "format": api_format})
                )
                dataset.resources.append(
                    Resource(
                        "export-" + fmt, url, fmt, {"format": fmt, "url": url}, "export"
                    )
                )
        elif view_type == "tabular" or kind in {"dataset", "filter"}:
            # SODA1 /rows exports were deprecated in October 2026. SODA3 full
            # public exports use GET without a row limit or application token.
            formats = ["csv", "tsv", "xlsx"]
            if any(
                c.get("dataTypeName")
                in {"point", "location", "multipolygon", "multiline"}
                for c in metadata.get("columns", [])
            ):
                formats.extend(["kml", "kmz"])
            for fmt in formats:
                url = f"{root}/api/v3/views/{quote(identifier, safe='')}/export.{fmt}"
                dataset.resources.append(
                    Resource(
                        "export-" + fmt, url, fmt, {"format": fmt, "url": url}, "export"
                    )
                )
        else:
            url = metadata.get("externalLink") or metadata.get("metadata", {}).get(
                "accessPoints", {}
            ).get("download")
            fmt = infer_format(url or "")
            reason = (
                None
                if url and fmt and fmt not in {"html", "htm"}
                else f"Asset type {kind or view_type!r} has no supported data export"
            )
            dataset.resources.append(
                Resource("asset", url, fmt, metadata, reason=reason)
            )
        for attachment in metadata.get("metadata", {}).get("attachments", []):
            asset = attachment.get("assetId")
            blob = attachment.get("blobId")
            url = attachment.get("url")
            if asset:
                url = f"{view}/files/{quote(str(asset), safe='')}?download=true"
            elif blob:
                url = f"{root}/api/assets/{quote(str(blob), safe='')}?download=true"
            elif url:
                url = urljoin(root, url)
            fmt = infer_format(
                url or "",
                attachment.get("mimeType") or attachment.get("mimetype"),
                attachment.get("filename", ""),
            )
            identifier = str(asset or blob or resource_id(attachment))
            dataset.resources.append(
                Resource(
                    "attachment-" + identifier,
                    url,
                    fmt,
                    attachment,
                    reason=None if url else "Attachment has no download URL",
                )
            )
        return dataset

    def close(self):
        for client in self.clients.values():
            client.close()
