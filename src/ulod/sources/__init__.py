from importlib import import_module

__all__ = ["Source", "CKAN", "SessionCKAN", "ODS", "SocrataClient", "cast_socrata_types"]


def __getattr__(name: str):
    if name == "Source":
        return import_module("ulod.sources.base").Source
    if name in {"CKAN", "SessionCKAN"}:
        return getattr(import_module("ulod.sources.ckan"), name)
    if name == "ODS":
        return import_module("ulod.sources.ods").ODS
    if name in {"SocrataClient", "cast_socrata_types"}:
        return getattr(import_module("ulod.sources.socrata" if name == "SocrataClient" else "ulod.sources.utils"), name)
    raise AttributeError(f"module 'ulod.sources' has no attribute {name!r}")
