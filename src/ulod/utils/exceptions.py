class HTTPResourceError(Exception):
    def __init__(self, url: str, status: int):
        self._url = url
        self._status = status

    def __str__(self):
        return f"Error status {self._status} with requested URL: {self._url}"


class TooLargeResourceError(Exception):
    def __init__(self, url: str, size: int, max_size: int):
        self._url = url
        self._size = size
        self._max_size = max_size

    def __str__(self):
        return f"Resource size {self._size} > max_size {self._max_size} for URL: {self._url}"


class IsHTMLError(Exception):
    def __init__(
        self,
        text: str,
    ):
        self._text = text

    def __str__(self):
        return (
            f"HTML-like elements detected in the first part of data: {self._text}. "
            "It is not possible to recognize it as a structured dataset."
        )


class IsZIPError(Exception):
    def __init__(self, url: str):
        self._url = url

    def __str__(self):
        return (
            f"Filename ends with '.zip': {self._url}. "
            "ZIP files are not recognized as structured datasets."
        )
