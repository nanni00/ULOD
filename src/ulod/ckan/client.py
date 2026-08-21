from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from threading import Lock
from typing import Any

import requests
import urllib3

from ulod.base import Source
from ulod.utils.exceptions import HTTPResourceError

__all__ = ["CKAN", "SessionCKAN", "StreamResponse"]


@dataclass
class StreamResponse:
    status: int
    headers: Mapping[str, str]
    _iter_content: Callable[[int], Iterator[bytes]]
    _close: Callable[[], None]

    def iter_content(self, chunk_size: int) -> Iterator[bytes]:
        return self._iter_content(chunk_size)

    def close(self) -> None:
        self._close()


def endpoint(name):
    def decorator(func):
        def wrapper(self, **kwargs):
            return self._base_method(name, **kwargs)

        wrapper.__name__ = name
        return wrapper

    return decorator


class CKAN(Source):
    source_type = "ckan"

    def __init__(
        self,
        base_url: str,
        action_url: str,
        headers: dict,
        connection_kw: dict | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.action_url = action_url
        self.final_url = f"{self.base_url}{action_url}"
        self.headers = headers
        self.connection_kw = connection_kw if connection_kw else {}

    def default_bulk_download_policy(self) -> dict[str, Any]:
        return {}

    def warmup_session(self, url: str | None = None) -> None:
        return None

    def close(self) -> None:
        return None

    def request_json(self, url: str) -> dict[str, Any]:
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

    def _make_request(self, url: str):
        return self.request_json(url)

    def _complete_url_with_kwargs(self, url, **kwargs):
        url += "&".join(
            (f"{x[0]}={x[1]}" for x in filter(lambda v: v[1] is not None, kwargs.items()))
        )

        return url

    def _base_method(self, action: str, **kwargs):
        action = self._complete_url_with_kwargs(f"{action}?", **kwargs)
        url = f"{self.final_url}/{action}"
        return self._make_request(url)

    @endpoint("package_search")
    def package_search(self, **kwargs):
        pass

    @endpoint("package_show")
    def package_show(self, **kwargs):
        pass

    @endpoint("package_list")
    def package_list(self, **kwargs):
        pass

    @endpoint("resource_show")
    def resource_show(self, **kwargs):
        pass

    @endpoint("resource_search")
    def resource_search(self, **kwargs):
        pass


class SessionCKAN(CKAN):
    def __init__(
        self,
        base_url: str,
        action_url: str,
        headers: dict,
        connection_kw: dict | None = None,
        session: requests.Session | None = None,
    ) -> None:
        super().__init__(base_url, action_url, headers, connection_kw)
        self._session = session or requests.Session()
        self._session_lock = Lock()

    def close(self) -> None:
        self._session.close()

    def warmup_session(self, url: str | None = None) -> None:
        target = url or self.base_url
        with self._session_lock:
            response = self._session.get(
                target, headers=self.headers, **self._requests_kwargs(stream=False)
            )
        try:
            if response.status_code >= 400:
                raise HTTPResourceError(target, response.status_code)
        finally:
            response.close()

    def request_json(self, url: str) -> dict[str, Any]:
        with self._session_lock:
            response = self._session.get(
                url, headers=self.headers, **self._requests_kwargs(stream=False)
            )
        try:
            if response.status_code >= 400:
                raise HTTPResourceError(url, response.status_code)
            return response.json()
        finally:
            response.close()

    def stream_request(self, url: str) -> StreamResponse:
        self._session_lock.acquire()
        try:
            response = self._session.get(
                url, headers=self.headers, **self._requests_kwargs(stream=True)
            )
        except Exception:
            self._session_lock.release()
            raise

        return StreamResponse(
            status=response.status_code,
            headers=response.headers,
            _iter_content=lambda chunk_size: response.iter_content(chunk_size),
            _close=self._build_stream_closer(response),
        )

    def _build_stream_closer(self, response: requests.Response) -> Callable[[], None]:
        def close() -> None:
            try:
                response.close()
            finally:
                self._session_lock.release()

        return close

    def _requests_kwargs(self, stream: bool) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"stream": stream}
        if "timeout" in self.connection_kw:
            kwargs["timeout"] = self.connection_kw["timeout"]
        if "redirect" in self.connection_kw:
            kwargs["allow_redirects"] = self.connection_kw["redirect"]
        if "verify" in self.connection_kw:
            kwargs["verify"] = self.connection_kw["verify"]
        if "cert" in self.connection_kw:
            kwargs["cert"] = self.connection_kw["cert"]
        if "proxies" in self.connection_kw:
            kwargs["proxies"] = self.connection_kw["proxies"]
        return kwargs
