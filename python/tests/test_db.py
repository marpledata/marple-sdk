import os
import pprint
import random
import time

from marple import DB

DB_TOKEN = "TODO"
STREAM_CSV = "Compulsory Salty Vaccine"  # Data x Island

db = DB(DB_TOKEN)

# Test basic built-in functions
db.check_connection()
pprint.pprint(db.get_streams())
pprint.pprint(db.get_datasets("charles river"))

# Test querying, using raw POST request
query = "select path, stream_id, metadata \nfrom mdb_default_dataset\nlimit 10;"
response = db.post("/query", json={"query": query})
print(response.json())

# Test uploading a file
dataset_id = db.push_file(
    STREAM_CSV,
    "tests/examples_race.csv",
    metadata={"source": "test_db.py"},
)
while True:
    status = db.get_status(STREAM_CSV, dataset_id)
    print(status)
    if status["import_status"] in ["FINISHED", "FAILED"]:
        break
    time.sleep(0.5)

# Test downloading a file
datasets = db.get_datasets(STREAM_CSV)
dataset = random.choice(datasets)
os.mkdir("tests/downloads")
db.download_original(STREAM_CSV, dataset["id"], destination="tests/downloads")
