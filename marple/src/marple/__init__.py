import requests
import requests.auth


class Marple:

    api_url = 'https://app.marpledata.com/api/v1'

    plugin_map = {
        'csv': 'csv_plugin',
        'txt': 'csv_plugin',
        'mat': 'mat_plugin',
        'h5': 'hdf5_plugin',
        'zip': 'hdf5_plugin',
        'bag': 'rosbag_plugin',
        'ulg': 'ulog_plugin'
    }

    def __init__(self, access_token):
        bearer_token = f"Bearer {access_token}"
        self.session = requests.Session()
        self.session.headers.update({"Authorization": bearer_token})

    def check_connection(self):
        r = self.session.get('{}/version'.format(self.api_url))
        if r.status_code != 200:
            raise Exception('Could not connect to server at {}'.format(self.api_url))
        return True

    def upload_data_file(self, file_path, marple_folder, plugin=None, metadata={}):
        file = open(file_path, 'rb')
        r = self.session.post('{}/library/file/upload'.format(self.api_url),
                              params={'path': marple_folder},
                              files={'file': file})
        source_id, path = r.json()['message']['source_id'], r.json()['message']['path']

        plugin = self.guess_plugin(file_path)
        body = {'path': path, 'plugin': plugin}
        self.session.post('{}/library/file/import'.format(self.api_url), json=body)
        return source_id

    def guess_plugin(self, file_path):
        extension = file_path.split('.')[0].lower()
        if extension in self.plugin_map:
            return self.plugin_map[extension]
        return 'csv_plugin'

    def check_import_status(self, source_id):
        r = self.session.get('{}/sources/status'.format(self.api_url), params={'id': source_id})
        return r.json()['message'][0]['status']

    def get_link(self, source_id, project_name, open_link=True):
        # make new share link
        body = {'workbook_name': project_name, 'source_ids': [source_id]}
        r = self.session.post('{}/library/share/new'.format(self.api_url), json=body)
        share_id = r.json()['message']

        # Generate clickable link in terminal
        r = self.session.get('{}/library/share/{}/link'.format(self.api_url, share_id))
        link = r.json()['message']
        print('View your data: {}'.format(link))
        return link
