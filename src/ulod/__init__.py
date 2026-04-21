"""
A very basic, unefficient, unsafe library to access some CKAN, Socrata and other Open Data endpoints.

At the time I am writing, there is no library that allows to access and download, even
with minimal setting and super simple functionalities, both these Open Data providers,
and in general Python libraries for CKAN are quite sparse or are not focused on some aspects
which instead are relevant for my work.
"""

from importlib import import_module

__all__ = ["bulk", "countries", "sources", "un", "wbo"]


def __getattr__(name: str):
    if name in __all__:
        return import_module(f"ulod.{name}")
    raise AttributeError(f"module 'ulod' has no attribute {name!r}")
