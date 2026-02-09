import requests


def validate_response(response: requests.Response, failure_message: str, check_status: bool = True) -> dict:
    if response.status_code == 400 or response.status_code == 500:
        raise Exception(f"{failure_message}: {response.json().get('error', 'Unknown error')}")
    if response.status_code != 200:
        response.raise_for_status()
    r_json = response.json()
    if check_status and r_json["status"] != "success":
        raise Exception(failure_message)
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
