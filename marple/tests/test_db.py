import pprint

from marple import DB

ACCESS_TOKEN = "TODO"

db = DB(ACCESS_TOKEN)
db.check_connection()
pprint.pprint(db.get_streams())
