import pprint

from marple import DB

ACCESS_TOKEN = "TODO"

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
response = db.push_file(
    "Compulsory Salty Vaccine",
    "tests/examples_race.csv",
    metadata={"source": "test_db.py"},
)
print(response)
