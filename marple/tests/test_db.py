import pprint

from marple import DB

ACCESS_TOKEN = "TODO"

db = DB(ACCESS_TOKEN)
db.check_connection()
pprint.pprint(db.get_streams())
pprint.pprint(db.get_datasets("charles river"))

query = "select path, stream_id, metadata \nfrom mdb_default_dataset\nlimit 10;"
response = db.post("/query", json={"query": query})
print(response.json())
