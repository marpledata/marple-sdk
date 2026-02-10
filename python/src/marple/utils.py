import requests


def validate_response(response: requests.Response, failure_message: str) -> dict:
    if response.status_code == 400:
        raise ValueError(f"{failure_message}: Bad request. {response.json().get('error', 'Unknown error')}")
    if response.status_code == 403:
        raise ValueError(f"{failure_message}: Invalid token.")
    if response.status_code == 405:
        raise ValueError(f"{failure_message}: Method not allowed.")
    if response.status_code == 500:
        raise ValueError(f"{failure_message}: {response.json().get('error', 'Unknown error')}")
    if response.status_code != 200:
        response.raise_for_status()
    r_json = response.json()
    if isinstance(r_json, dict) and r_json.get("status", "success") not in ["success", "healthy"]:
        raise ValueError(failure_message)
    return r_json


class DBSession:
    def __init__(self, api_token: str, api_url: str, datapool: str, cache_folder: str):
        self.api_token = api_token
        self.api_url = api_url
        self.datapool = datapool
        self.cache_folder = cache_folder

        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.api_token}"})
        self.session.headers.update({"X-Request-Source": "sdk/python"})

    def get(self, url: str, *args, **kwargs) -> requests.Response:
        return self.session.get(f"{self.api_url}{url}", *args, **kwargs)

    def post(self, url: str, *args, **kwargs) -> requests.Response:
        return self.session.post(f"{self.api_url}{url}", *args, **kwargs)

    def patch(self, url: str, *args, **kwargs) -> requests.Response:
        return self.session.patch(f"{self.api_url}{url}", *args, **kwargs)

    def delete(self, url: str, *args, **kwargs) -> requests.Response:
        return self.session.delete(f"{self.api_url}{url}", *args, **kwargs)
