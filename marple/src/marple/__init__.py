import requests
import requests.auth
import os
import pandas as pd


class Marple:

    api_url = 'https://app.marpledata.com/api/v1'

    plugin_map = {
        'csv': 'csv_plugin',
        'txt': 'csv_plugin',
        'mat': 'mat_plugin',
        'h5': 'hdf5_plugin',
        'zip': 'csv_zip_plugin',
        'bag': 'rosbag_plugin',
        'ulg': 'ulog_plugin'
    }

    def __init__(self, access_token):
        if access_token == '':
            raise Exception('Invalid access token')
        bearer_token = f"Bearer {access_token}"
        self.session = requests.Session()
        self.session.headers.update({"Authorization": bearer_token})
        self.data = {}

    # User functions #

    def check_connection(self):
        r = self.session.get('{}/version'.format(self.api_url))
        if r.status_code != 200:
            raise Exception(r.json()['message'])
        return True

    def upload_data_file(self, file_path, marple_folder, plugin=None, metadata={}):
        file = open(file_path, 'rb')
        r = self.session.post('{}/library/file/upload'.format(self.api_url),
                              params={'path': marple_folder},
                              files={'file': file})
        if r.status_code != 200:
            raise Exception(r.json()['message'])
        source_id, path = r.json()['message']['source_id'], r.json()['message']['path']

        # convert to name, value structure
        if metadata:
            metadata_marple = [{'name': key, 'value': value} for key, value in metadata.items()]
            r = self.session.post('{}/library/metadata'.format(self.api_url),
                                  json={'source_id': source_id, 'metadata': metadata_marple})
            if r.status_code != 200:
                raise Exception(r.json()['message'])

        if plugin is None:
            plugin = self._guess_plugin(file_path)
        body = {'path': path, 'plugin': plugin}
        self.session.post('{}/library/file/import'.format(self.api_url), json=body)
        if r.status_code != 200:
            raise Exception(r.json()['message'])
        return source_id

    def upload_dataframe(self, dataframe, name, marple_folder, metadata={}):
        file_name = f'{name}.csv'
        dataframe.to_csv(file_name, sep=',', index=False)
        source_id = self.upload_data_file(file_name, marple_folder, metadata=metadata)
        os.remove(file_name)
        return source_id

    def add_data(self, data_dict):
        if self.data == {}:
            self.data = {s: [v] for s, v in data_dict.items()}
        else:
            for key in data_dict:
                if key not in self.data:
                    raise Exception(f'Key {key} not known in data.')
                self.data[key].append(data_dict[key])

    def clear_data(self):
        self.data = {}

    def send_data(self, name, marple_folder, metadata={}):
        df = pd.DataFrame.from_dict(self.data)
        self.clear_data()
        return self.upload_dataframe(df, name, marple_folder, metadata={})

    def check_import_status(self, source_id):
        r = self.session.get('{}/sources/status'.format(self.api_url), params={'id': source_id})
        if r.status_code != 200:
            raise Exception(r.json()['message'])
        return r.json()['message'][0]['status']

    def get_link(self, source_id, project_name, open_link=True):
        # make new share link
        body = {'workbook_name': project_name, 'source_ids': [source_id]}
        r = self.session.post('{}/library/share/new'.format(self.api_url), json=body)
        if r.status_code != 200:
            raise Exception(r.json()['message'])
        share_id = r.json()['message']

        # Generate clickable link in terminal
        r = self.session.get('{}/library/share/{}/link'.format(self.api_url, share_id))
        if r.status_code != 200:
            raise Exception(r.json()['message'])
        link = r.json()['message']
        print('View your data: {}'.format(link))
        return link

    # Internal functions #

    def _guess_plugin(self, file_path):
        extension = file_path.split('.')[0].lower()
        if extension in self.plugin_map:
            return self.plugin_map[extension]
        return 'csv_plugin'
