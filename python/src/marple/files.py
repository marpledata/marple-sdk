import os
import webbrowser

import pandas as pd
import requests
import requests.auth
from requests import Response
from requests.exceptions import ConnectionError

SAAS_URL = "https://app.marpledata.com/api/v1"
DEFAULT_IMPORT_CONFIG = {"common": [], "signals_groups": []}


class Marple:

    plugin_map = {
        "csv": "csv_plugin",
        "txt": "csv_plugin",
        "mat": "mat_plugin",
        "h5": "hdf5_plugin",
        "zip": "csv_zip_plugin",
        "bag": "rosbag_plugin",
        "ulg": "ulog_plugin",
    }

    def __init__(self, access_token, api_url=SAAS_URL):
        self._deprecation_warning()

        if access_token == "":
            raise Exception("Invalid access token")
        bearer_token = f"Bearer {access_token}"
        self.session = requests.Session()
        self.session.headers.update({"Authorization": bearer_token})
        self._api_url = api_url
        self._data = {}

    # User functions #

    def get(self, url: str, *args, **kwargs) -> Response:
        return self.session.get(f"{self._api_url}{url}", *args, **kwargs)

    def post(self, url: str, *args, **kwargs) -> Response:
        return self.session.post(f"{self._api_url}{url}", *args, **kwargs)

    def patch(self, url: str, *args, **kwargs) -> Response:
        return self.session.patch(f"{self._api_url}{url}", *args, **kwargs)

    def delete(self, url: str, *args, **kwargs) -> Response:
        return self.session.delete(f"{self._api_url}{url}", *args, **kwargs)

    def check_connection(self):
        msg_fail_connect = "Could not connect to server at {}".format(self._api_url)
        msg_fail_auth = "Could not authenticate with token"

        try:
            # unauthenticated endpoints
            r = self.get("/version")
            if r.status_code != 200:
                raise Exception(msg_fail_connect)

            # authenticated endpoint
            r = self.get("/")
            if r.status_code != 200:
                raise Exception(msg_fail_auth)

        except ConnectionError:
            raise Exception(msg_fail_connect)

        return True

    def upload_data_file(
        self,
        file_path,
        marple_folder="/",
        plugin=None,
        metadata={},
        config=DEFAULT_IMPORT_CONFIG,
    ):
        file = open(file_path, "rb")
        r = self.post(
            "/library/file/upload", params={"path": marple_folder}, files={"file": file}
        )
        if r.status_code != 200:
            r.raise_for_status()
        source_id, path = r.json()["message"]["source_id"], r.json()["message"]["path"]

        # convert to name, value structure
        if metadata:
            r = self.post(
                "/library/metadata", json={"source_id": source_id, "metadata": metadata}
            )
            if r.status_code != 200:
                r.raise_for_status()

        if plugin is None:
            plugin = self._guess_plugin(file_path)
        body = {"path": path, "plugin": plugin, "config": config}
        self.post("/library/file/import", json=body)
        if r.status_code != 200:
            r.raise_for_status()
        return source_id

    def upload_dataframe(self, dataframe, name, marple_folder="/", metadata={}):
        file_name = f"{name}.csv"
        dataframe.to_csv(file_name, sep=",", index=False)
        source_id = self.upload_data_file(file_name, marple_folder, metadata=metadata)
        os.remove(file_name)
        return source_id

    def add_data(self, data_dict):
        if self._data == {}:
            self._data = {s: [v] for s, v in data_dict.items()}
        else:
            for key in data_dict:
                if key not in self._data:
                    raise Exception(f"Key {key} not known in data.")
                self._data[key].append(data_dict[key])

    def clear_data(self):
        self._data = {}

    def send_data(self, name, marple_folder="/", metadata={}):
        df = pd.DataFrame.from_dict(self._data)
        self.clear_data()
        return self.upload_dataframe(df, name, marple_folder, metadata)

    def check_import_status(self, source_id):
        r = self.get("/sources/status", params={"id": source_id})
        if r.status_code != 200:
            r.raise_for_status()
        return r.json()["message"][0]["status"]

    def get_link(self, source_id, project_name, open_link=True):
        # make new share link
        body = {"workbook_name": project_name, "source_ids": [source_id]}
        r = self.post("/library/share/new", json=body)
        if r.status_code != 200:
            r.raise_for_status()
        share_id = r.json()["message"]

        # Generate clickable link in terminal
        r = self.get(f"/library/share/{share_id}/link")
        if r.status_code != 200:
            r.raise_for_status()
        link = r.json()["message"]
        if open_link:
            webbrowser.open(link)
        print(f"View your data: {link}")
        return link

    # Internal functions #

    def _guess_plugin(self, file_path):
        extension = file_path.split(".")[-1].lower()
        if extension in self.plugin_map:
            return self.plugin_map[extension]
        return "csv_plugin"

    def _deprecation_warning(self):
        print(
            "Marple is launching their next generation of products: Marple Insight and Marple DB!"
        )
        print("This part of the SDK will be deprecated in the future.")
        print("Find out more at https://www.marpledata.com")
