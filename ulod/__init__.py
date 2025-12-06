"""
Un-advanced Library for Open Data

A very basic, unefficient, unsafe library to access some CKAN, Socrata and other Open Data endpoints.

At the time I am writing, there is no library that allows to access and download, even
with minimal setting and super simple functionalities, both these Open Data providers,
and in general Python libraries for CKAN are quite sparse or are not focused on some aspects which instead are relevant for my work.
"""

from . import bulk, ckan, socrata, un, wbo

__all__ = ["bulk", "ckan", "socrata", "un", "wbo"]
