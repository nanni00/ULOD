# class RequestFailedError(Exception):
#     def __init__(self, url: str, response: )


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
