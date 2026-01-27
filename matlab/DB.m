% DB.m
classdef DB
  properties
    api_url
    api_key
    workspace
    datapool
    streams % Cache for available streams
  end

  methods (Static, Access = private)
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
  end

  methods (Static)
    function obj = from_config()
      % Static constructor using config file
      cfg = DB.read_config();
      obj = DB(cfg.api_url, cfg.api_key);

      % Set additional properties from config
      obj.workspace = cfg.workspace;
      if isfield(cfg,'datapool')
            obj.datapool = cfg.datapool;
      else
            obj.datapool = "default";
      end

      obj.streams = obj.get_streams();
    end
  end

  methods (Access = private)
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

    function data = query(obj, query_string)
      endpoint = '/query';
      body = struct('query', query_string, 'hot', true);
      data = obj.make_request('POST', endpoint, body);
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

    function out = find_stream_and_dataset_id(obj, dataset_path)
      query_string = sprintf('select id, stream_id from mdb_%s_dataset where path = ''%s''', obj.datapool, dataset_path);
      res = obj.query(query_string);
      if numel(res.data) ~= 2 
        error('Dataset "%s" not found', dataset_path);
      end
      out.stream_id = res.data(2);
      out.dataset_id = res.data(1);
    end

    function signal_id = find_signal_id(obj, signal_name)
      query_string = sprintf('select id from mdb_%s_signal_enum where name = ''%s''', obj.datapool, signal_name);
      res = obj.query(query_string);
      if numel(res.data) ~= 1
        error('Signal "%s" not found', signal_name);
      end
      signal_id = res.data;
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

    function T = get_data(obj, dataset_path, signal_name, is_text)
      arguments
        obj
        dataset_path
        signal_name
        is_text logical = false
      end
      out = obj.find_stream_and_dataset_id(dataset_path);
      signal_id = obj.find_signal_id(signal_name);
      cache = sprintf('_marplecache/%s/%s/dataset=%d/signal=%d', obj.workspace, obj.datapool, out.dataset_id, signal_id);

      if ~isfolder(cache)
        mkdir(cache)
        endpoint = sprintf('/stream/%d/dataset/%d/signal/%d/path', out.stream_id, out.dataset_id, signal_id);
        res = obj.make_request('GET', endpoint);
        for i = 1:length(res.paths)
          url = res.paths{i};
          [~, name, ext] = fileparts(extractBefore(url, '?'));
          parquet_name = [name ext];
          cache_path = sprintf('%s/%s', cache, parquet_name);
          websave(cache_path, url);
        end
      end

      ds = parquetDatastore(cache);
      T = readall(ds);

      if is_text
        T = removevars(T, 'value');
        T.Properties.VariableNames{'value_text'} = signal_name;
      else
        T = removevars(T, 'value_text');
        T.Properties.VariableNames{'value'} = signal_name;
      end
    end

    function clear_cache(obj)
      cache = sprintf('_marplecache/%s/%s', obj.workspace, obj.datapool);
      if isfolder(cache)
        rmdir(cache, 's');
      end
    end
  end
  
end
