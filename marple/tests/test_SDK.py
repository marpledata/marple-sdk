import shutil
import time
import pandas as pd
import numpy as np

from src.marple import Marple
# from marple import Marple

ACCESS_TOKEN = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IkRrXzRWOWxwYUtRQmZVZ2ZpbzZIciJ9.eyJodHRwczovL21hcnBsZWRhdGEuY29tL2VtYWlsX3ZlcmlmaWVkIjp0cnVlLCJpc3MiOiJodHRwczovL21hcnBsZS5ldS5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMDQzMTQ1Njg1NTIxMjIwNDMyMzMiLCJhdWQiOlsiaHR0cHM6Ly9hcHAuZ2V0bWFycGxlLmlvL2FwaSIsImh0dHBzOi8vbWFycGxlLmV1LmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE2NTg0MTUzMTksImV4cCI6MTY2MTAwNzMxOSwiYXpwIjoiVm81alNlclZoUUxxeWJRT1dkME03aDg2MUU2RE1WVkciLCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIn0.V7qDEU7t-WweulmXhsTl9OnCZrUTmztM8NwfyROKkaLaEiXkWthgzcuReR7HNt-SsawiOh75LylpQypNMXIm_pEGku4oUBgs3_cNod7-OulbQ2_LLBA6D-4thpGgECrFcCmmAGGCKGVOCjnmd-vjCt-6eZSLcd1s4jiSVb9lhkLsbYEi3zICJMoBuygniO6BIstejzfKNrwDFg3U28Lde6rSYt3dmA4zQ5TWP9ka-sNqSjsJl9-2X4yERoyYLCRnyQe88hMnfXb_w2nypQklYzAVFZtEYm65LzUSoUqfCfw26jxh_VhYo_H0jphLbw-aao6sWtMa4dUa9aLGznflxA'
EXAMPLE_FILE_PATH = 'tests\\examples_race.csv'
PROJECT_NAME = 'api-project'

# create a copy of the file
new_file_path = f'example_{int(time.time())}.csv'
shutil.copy(EXAMPLE_FILE_PATH, new_file_path)


m = Marple(ACCESS_TOKEN)

# Check connection
m.check_connection()
print('Connection OK')

# Upload a file
source_id = m.upload_data_file(new_file_path, 'API_test')
print(f'Source id: {source_id} uploaded')

# get a link to the data and project
share_link = m.get_link(source_id, PROJECT_NAME)

# Generate some data and write it to Marple
data = {
    'time': range(0,100),
    'signal 1': range(0,100),
    'signal 2': [np.sin(i / (2 * np.pi)) for i in range(0,100)]
}
df = pd.DataFrame.from_dict(data)
target_name = f'example_fromdata_{int(time.time())}'
source_id = m.upload_dataframe(df, target_name, 'API_test')
print(f'Source id: {source_id} uploaded')

# Add data iteratively
for i in range(0, 100):
    data_dict = {
        'time': i,
        'signal 1': i,
        'signal 2': np.sin(i / (2 * np.pi))
    }
    m.add_data(data_dict)
target_name = f'example_add_data_{int(time.time())}'
source_id = m.send_data(target_name, 'API_test')
print(f'Source id: {source_id} uploaded')
