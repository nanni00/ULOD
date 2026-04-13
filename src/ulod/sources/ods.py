import json
from typing import Optional

import urllib3

from ulod.sources.base import Source


def endpoint(name):
    def decorator(func):
        def wrapper(self, **kwargs):
            return self._base_method(name, **kwargs)

        wrapper.__name__ = name
        return wrapper

    return decorator


# A possible option for implementing subclasses is
# python functools.partial, but in this way we lose the possibility
# to override methods for specific cases
class ODS(Source):
    source_type = "ods"

    def __init__(
        self,
        base_url: str,
        action_url: str,
        headers: dict,
        connection_kw: Optional[dict] = None,
    ) -> None:
        self.base_url = base_url
        self.action_url = action_url
        self.final_url = f"{base_url}{action_url}"
        self.headers = headers
        self.connection_kw = connection_kw if connection_kw else {}

    def _make_request(self, url: str):
        """ "Do a GET request"""
        response = urllib3.request(
            "GET", url, headers=self.headers, **self.connection_kw
        )

        try:
            decoded = response.json()
        except json.decoder.JSONDecodeError:
            decoded = response.data.decode(
                "utf-8-sig"
            )  # json.loads(response.data.decode("utf-8-sig"))

        return decoded

    def _complete_url_with_kwargs(self, url: str, **kwargs) -> str:
        """Resolve path params like {dataset_id}, then append remaining as query string."""
        import re

        path_params = set(re.findall(r"\{(\w+)\}", url))
        for param in path_params:
            if param not in kwargs:
                raise ValueError(f"Missing required path parameter: '{param}'")
            url = url.replace(f"{{{param}}}", str(kwargs.pop(param)))

        if "limit" in kwargs and kwargs["limit"] > 100:
            raise ValueError("ODS expects -1<=limit<=100")
        query_string = "&".join(f"{k}={v}" for k, v in kwargs.items() if v is not None)
        if query_string:
            url += f"{query_string}"

        return url

    def _base_method(self, action: str, **kwargs):
        action = self._complete_url_with_kwargs(f"/{action}?", **kwargs)
        url = f"{self.final_url}{action}"
        return self._make_request(url)

    @endpoint("/catalog/datasets")
    def catalog_datasets(self, **kwargs):
        pass

    @endpoint("/catalog/exports")
    def catalog_exports(self, **kwargs):
        pass

    @endpoint("/catalog/exports/csv")
    def catalog_exports_csv(self, **kwargs):
        pass

    @endpoint("/catalog/datasets/{dataset_id}/exports/{format}")
    def export_dataset_in_format(self, **kwargs):
        pass
