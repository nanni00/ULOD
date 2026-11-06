"""Minimal OpenDataSoft Explore v2.1 catalog, exports and attachments adapter."""

from urllib.parse import quote, urljoin

from .common import Dataset, Resource, infer_format, paged_entries, resource_id


class ODSAdapter:
    def __init__(self, url, cfg, http):
        self.url, self.cfg, self.http = url, cfg, http
        self.api = url + "/api/explore/v2.1/catalog/datasets"

    def page(self, offset):
        page = self.http.json(
            self.api,
            params={
                "limit": min(self.cfg.page_size, 100),
                "offset": offset,
                "order_by": "dataset_id",
            },
        )
        return paged_entries(page, "results", "total_count", offset)

    @staticmethod
    def identifier(entry):
        return str(entry["dataset_id"])

    def describe(self, entry):
        identifier = self.identifier(entry)
        endpoint = f"{self.api}/{quote(identifier, safe='')}"
        metadata = self.http.json(endpoint)
        dataset = Dataset(identifier, metadata)
        if metadata.get("has_records", True):
            try:
                exports = self.http.json(endpoint + "/exports")
                dataset.extra_metadata["exports"] = exports
                for link in exports["links"]:
                    fmt = link.get("rel")
                    if not fmt or fmt in {"self", "next", "previous", "alternate"}:
                        continue
                    dataset.resources.append(
                        Resource(
                            "export-" + fmt,
                            urljoin(endpoint, link["href"]),
                            "zip" if fmt == "shp" else infer_format(link["href"], fmt),
                            link,
                            "export",
                        )
                    )
                if not any(r.kind == "export" for r in dataset.resources):
                    dataset.resources.append(
                        Resource("data", None, reason="No dataset export is advertised")
                    )
            except Exception as error:
                dataset.errors.append(
                    f"Export discovery: {type(error).__name__}: {error}"
                )
        try:
            attachments = self.http.json(endpoint + "/attachments")
            dataset.extra_metadata["attachments"] = attachments
            for attachment in attachments["attachments"]:
                raw = attachment.get("metas", attachment)
                url = attachment.get("href") or raw.get("url")
                dataset.resources.append(self.file(raw, url, "attachment", endpoint))
        except Exception as error:
            dataset.errors.append(
                f"Attachment discovery: {type(error).__name__}: {error}"
            )
            # Preserve/download attachments already supplied by catalog metadata.
            for raw in metadata.get("attachments", []):
                dataset.resources.append(
                    self.file(raw, raw.get("url"), "attachment", endpoint)
                )
        for raw in metadata.get("alternative_exports", []):
            dataset.resources.append(
                self.file(
                    raw, raw.get("url") or raw.get("href"), "alternative", endpoint
                )
            )
        if not dataset.resources and not dataset.errors:
            dataset.resources.append(
                Resource(
                    "data",
                    None,
                    reason="Metadata-only dataset; no downloadable data advertised",
                )
            )
        return dataset

    @staticmethod
    def file(raw, url, kind, endpoint):
        url = urljoin(endpoint, url) if url else None
        fmt = infer_format(
            url or "",
            raw.get("mimetype") or raw.get("mime-type") or raw.get("format"),
            raw.get("title", ""),
        )
        return Resource(
            f"{kind}-{resource_id(raw)}",
            url,
            fmt,
            raw,
            reason=None if url else "File has no download URL",
        )

    def close(self):
        pass
