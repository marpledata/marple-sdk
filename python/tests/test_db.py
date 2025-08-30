import pprint

from marple import DB

ACCESS_TOKEN = "TODO"
STREAM_CSV = "Compulsory Salty Vaccine"

db = DB(ACCESS_TOKEN)

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
