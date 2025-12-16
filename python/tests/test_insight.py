import os
import random

from marple import DB, Insight

if __name__ == "__main__":
    INSIGHT_TOKEN = "TODO"
    DB_TOKEN = "TODO"
    STREAM_CSV = "Compulsory Salty Vaccine"  # Data x Island

    insight = Insight(INSIGHT_TOKEN)
    db = DB(DB_TOKEN)

    # Test export
    datasets = db.get_datasets(STREAM_CSV)
    dataset = random.choice(datasets)

    os.mkdir("tests/downloads")
    insight.export_mdb(stream_id, dataset["id"], format="h5", destination="tests/downloads")
