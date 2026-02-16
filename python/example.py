import os
import re

import dotenv
from src.marple.db import DB, Dataset

dotenv.load_dotenv()
api_token = os.getenv("MDB_TOKEN")
if api_token is None:
    raise ValueError("MDB_TOKEN environment variable not set")

url = os.getenv("MDB_API_URL")
db = DB(api_token, url)
db.check_connection()

stream_name = "CSV Stream"
db.get_stream(stream_name)
if stream_name not in [stream.name for stream in db.get_streams()]:
    db.create_stream(stream_name)

stream = db.get_stream(stream_name)
stream.push_file("examples_race.csv", metadata={"car_id": 1, "track": "track_1", "weather": "sunny"})
stream.push_file("examples_race.csv", metadata={"car_id": 2, "track": "track_1", "weather": "cloudy"})
stream.push_file("examples_race.csv", metadata={"car_id": 3, "track": "track_1", "weather": "rainy"})
stream.push_file("examples_race.csv", metadata={"car_id": 1, "track": "track_2", "weather": "sunny"})
stream.push_file("examples_race.csv", metadata={"car_id": 2, "track": "track_2", "weather": "sunny"})

datasets = db.get_datasets()  # Get all datasets across all streams
datasets = datasets.where_metadata({"car_id": [1, 2], "track": "track_1"})
datasets = datasets.wait_for_import()
datasets = datasets.where_imported() # Only keep datasets that have been imported
datasets = datasets.where_signal("car.speed", "max", greater_than=75)  # max(speed) > 75
datasets = datasets.where_signal("car.engine.temp", "mean", greater_than=30)  # avg(temp) > 30
datasets = datasets.where_dataset("n_datapoints", greater_than=100000)  # Many datapoints


def custom_filter_function(dataset: Dataset) -> bool:
    return (
        dataset.metadata.get("weather") == "sunny"
        or dataset.get_signal("car.engine.NGear").stats.get("avg", 0) ** 2 > 16
    )


datasets = datasets.where(custom_filter_function)
signals = [
    "car.speed",
    "car.engine.temp",
    re.compile("car.wheel.*.speed"),
    re.compile("car.wheel.*.trq"),
]
for dataset, data in datasets.get_data(
    signals=signals,
    resample_rule="0.17s",
):
    print(data)
    break

    # machine_learning_model.train(data)  # Train a machine learning model on the data
