% DB.m
classdef DB
  properties
    s3_access_key
    s3_secret_key
    s3_region
    s3_bucket
    api_url
    api_key
    workspace
    datapool
    streams % Cache for available streams
  end

  methods (Static)
    function cfg = read_config()
      % Read JSON configuration file from same directory as this class
      json_path = fullfile(fileparts(mfilename('fullpath')), 'config.json');
      if ~isfile(json_path)
        error('Configuration file not found: %s', json_path);
      end
      fid = fopen(json_path, 'r');
      raw = fread(fid, '*char')';
      fclose(fid);
      cfg = jsondecode(raw);
    end

    function obj = from_config()
      % Static constructor using config file
      cfg = DB.read_config();
      obj = DB(cfg.api_url, cfg.api_key);

      % Set additional properties from config
      obj.s3_access_key = cfg.s3_access_key;
      obj.s3_secret_key = cfg.s3_secret_key;
      obj.s3_region = cfg.s3_region;
      obj.s3_bucket = cfg.s3_bucket;
      obj.workspace = cfg.workspace;
      obj.datapool = cfg.datapool;

      setenv("AWS_ACCESS_KEY_ID", obj.s3_access_key)
      setenv("AWS_SECRET_ACCESS_KEY", obj.s3_secret_key)
      setenv("AWS_DEFAULT_REGION", obj.s3_region)

      obj.streams = obj.get_streams();
    end
  end

  methods
    function obj = DB(api_url, api_key)
      obj.api_url = api_url;
      obj.api_key = api_key;
    end

    function status = health(obj)
      endpoint = '/health';
      status = obj.make_request('GET', endpoint);
    end

    function streams = get_streams(obj)
      % Get all available streams and cache them
      endpoint = '/streams';
      response = obj.make_request('GET', endpoint);
      % actual data is nested in this key
      obj.streams = response.streams;
      streams = obj.streams;
    end

    function stream_id = find_stream_id(obj, stream_name)
      for i = 1:length(obj.streams)
        if strcmpi(obj.streams(i).name, stream_name)
          stream_id = obj.streams(i).id;
          return;
        end
      end

      % If we get here, stream wasn't found
      available_names = strjoin(arrayfun(@(s) s.name, obj.streams, 'UniformOutput', false), ', ');
      error('Stream "%s" not found. Available streams are: %s', stream_name, available_names);
    end

    function datasets = get_datasets(obj, stream_name)
      % Get datasets for a stream by name
      stream_id = obj.find_stream_id(stream_name);
      endpoint = sprintf('/stream/%d/datasets', stream_id);
      datasets = obj.make_request('GET', endpoint);
    end

    function signals = get_signals(obj, stream_name, dataset_id)
      % Get signals for a stream by name and dataset ID
      stream_id = obj.find_stream_id(stream_name);
      endpoint = sprintf('/stream/%d/dataset/%d/signals', stream_id, dataset_id);
      signals = obj.make_request('GET', endpoint);
    end

    function T = get_data(obj, dataset_name, signal_name, is_text)
      arguments
        obj
        dataset_name
        signal_name
        is_text logical = false
      end

      url_structure = 's3://%s/cold/%s/%s/dataset=%s/signal=%s/*.parquet';
      s3_url = sprintf(url_structure, obj.s3_bucket, obj.workspace, obj.datapool, dataset_name, signal_name);

      local_structure = '_marplecache/%s/%s/dataset=%s/signal=%s/';
      local_dir = sprintf(local_structure, obj.workspace, obj.datapool, dataset_name, signal_name);
      T = obj.get_parquet(s3_url, local_dir);

      if is_text
        T = removevars(T, 'value');
        T.Properties.VariableNames{'value_text'} = signal_name;
      else
        T = removevars(T, 'value_text');
        T.Properties.VariableNames{'value'} = signal_name;
      end

    end

    function T = get_parquet(obj, s3_url, local_path)
      [parentPath, ~, ~] = fileparts(local_path);

      % works as caching
      if ~isfolder(parentPath)
        mkdir(parentPath);
        copyfile(s3_url, local_path); % works with s3 URLs since R2021b probably
      end

      ds = parquetDatastore(local_path);
      T = readall(ds);
    end

    function response = make_request(obj, method, endpoint, data)
      % Make HTTP request to API
      % method: 'GET' or 'POST'
      % endpoint: API endpoint starting with /
      % data: struct for POST body (optional)

      headers = {
        'Authorization', ['Bearer ' obj.api_key];
        'X-Request-Source', 'sdk/matlab'
      };
      options = weboptions('HeaderFields', headers, ...
                         'ContentType', 'json', ...
                         'RequestMethod', method);
                        % Combine base URL and endpoint
                        url = [obj.api_url endpoint];
      try
          if strcmp(method, 'GET')
              response = webread(url, options);
          else
              response = webwrite(url, data, options);
          end
      catch ME
          error('API request failed: %s', ME.message);
      end
    end

  end
end
