# Marple SDK

A simple SDK to upload data to Marple

## Installation
Install the Marple SDK using:

`pip install marpledata`

Import the package using:

`import marple`

## Usage
### Setup
Create a Marple connection using:

`from marple import Marple`

`m = Marple(ACCESS_TOKEN)`

You can find your personal access token in your account settings.

To check your connection, use:

`m.check_connection()`

### Upload data files
If your data is already in a file format, use this function to upload the data to Marple.

`source_id = m.upload_data_file(file_path, marple_folder)`

- `file_path`: the file_path of your data set
- `marple_folder`: in what folder you would like to upload the data

### Upload a dataframe
If your data is in a pandas dataframe, use this function to upload the data to Marple.

`source_id = m.upload_data_pandas(dataframe, target_name, marple_folder)`

- `target_name`: the target name for the dataset, this is how it will appear in Marple.

### Add and send data
You can also use the Marple SDK to add data piece by piece and send it to Marple.
First use:

`m.add_data(data_dict)`

- `data_dict` = dictionary with signal, value pairs in it.
  Example: `{'time': 2, 'signal 1': 0, 'signal 2': 5}`

Once all the data has been added, use:

`source_id = m.send_data(target_name, marple_folder)`

### Get a link to the data
You can generate a link to Marple that opens the data and a project immediately. This can be very useful to see the results of a simulation directly.

`link = m.get_link(source_id, project_name)`

- `source_id`: identifier for the data set, is returned by all above functions.
- `project_name`: name of the project that you want to open the data in.