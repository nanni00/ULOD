"""Example resource predicate: tabular formats with a hard selection budget."""

from ulod.bulk.common import infer_format


def tabular_files(formats, max_downloads):
    """Select at most max_downloads matching resources during one run.

    Create a fresh predicate for each download_catalog call. Existing files and
    failed downloads count toward this selection budget too. Metadata collection
    continues up to max_datasets; this predicate only limits data files.
    """
    selected = 0

    def accept(metadata):
        nonlocal selected
        url = metadata.get("url") or metadata.get("href") or ""
        declared = (
            metadata.get("format")
            or metadata.get("mimetype")
            or metadata.get("mime-type")
            or metadata.get("mimeType")
            or metadata.get("blobMimeType")
            or metadata.get("rel")
        )
        filename = metadata.get("filename") or metadata.get("blobFilename") or ""
        fmt = infer_format(url, declared, filename)
        if fmt not in formats or selected >= max_downloads:
            return False
        selected += 1
        return True

    return accept
