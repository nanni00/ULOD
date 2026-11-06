from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass


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
