import requests


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
class CKAN:
    def __init__(self, base_url: str, action_url: str, headers: dict) -> None:
        self.base_url = base_url
        self.action_url = action_url
        self.final_url = f"{base_url}{action_url}"
        self.headers = headers

    def _make_request(self, url: str):
        """ "Do a GET request"""
        response = requests.get(url, headers=self.headers)
        assert response.status_code == 200

        return response.json()

    def _complete_url_with_kwargs(self, url, **kwargs):
        url += "&".join(
            map(
                lambda x: f"{x[0]}={x[1]}",
                filter(lambda v: v[1] is not None, kwargs.items()),
            )
        )

        return url

    def _base_method(self, action: str, **kwargs):
        action = self._complete_url_with_kwargs(f"/{action}?", **kwargs)
        url = f"{self.final_url}{action}"
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
