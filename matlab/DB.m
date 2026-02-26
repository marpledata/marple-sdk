% DB.m
classdef DB
  properties
    api_url
    api_key
    workspace
    datapool
    streams % Cache for available streams
  end

  properties (Constant, Access = private)
    TRANSCODE_VERSION = 'v0.1.0'
    TRANSCODE_BASE_URL = 'https://github.com/marpledata/marple-sdk/releases/download/parquet-transcode'
  end

  methods (Static, Access = private)
    function cfg = read_config()
      json_path = fullfile(fileparts(mfilename('fullpath')), 'config.json');
      if ~isfile(json_path)
        error('Configuration file not found: %s', json_path);
      end
      fid = fopen(json_path, 'r');
      raw = fread(fid, '*char')';
      fclose(fid);
      cfg = jsondecode(raw);
    end

    function bin_path = ensure_binary()
      arch = computer('arch');
      switch arch
        case 'win64'
          platform = 'windows-x64';
          ext = '.exe';
        case 'maca64'
          platform = 'darwin-arm64';
          ext = '';
        case 'maci64'
          platform = 'darwin-x64';
          ext = '';
        case 'glnxa64'
          platform = 'linux-x64';
          ext = '';
        otherwise
          error('Unsupported platform: %s', arch);
      end

      bin_name = sprintf('parquet-transcode-%s-%s%s', DB.TRANSCODE_VERSION, platform, ext);
      bin_dir = fullfile(fileparts(mfilename('fullpath')), 'bin');
      bin_path = fullfile(bin_dir, bin_name);

      if isfile(bin_path)
        return;
      end

      if ~isfolder(bin_dir)
        mkdir(bin_dir);
      end

      url = sprintf('%s-%s/%s', DB.TRANSCODE_BASE_URL, DB.TRANSCODE_VERSION, bin_name);
      fprintf('Downloading %s for %s...\n', bin_name, arch);
      websave(bin_path, url);

      if ~strcmp(arch, 'win64')
        system(sprintf('chmod +x "%s"', bin_path));
      end
    end

    function transcode_cache(cache_dir)
      bin_path = DB.ensure_binary();
      [status, msg] = system(sprintf('"%s" "%s"', bin_path, cache_dir));
      if status ~= 0
        error('Parquet transcode failed: %s', msg);
      end
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
    function response = make_request(obj, method, endpoint, data, query_params)
      arguments
        obj
        method
        endpoint
        data = []
        query_params = struct()
      end

      headers = {
        'Authorization', ['Bearer ' obj.api_key];
        'X-Request-Source', 'sdk/matlab'
      };
      options = weboptions('HeaderFields', headers, ...
                         'ContentType', 'json', ...
                         'RequestMethod', method);
      url = [obj.api_url endpoint];
      try
          if strcmp(method, 'GET')
              qp_args = namedargs2cell(query_params);
              response = webread(url, qp_args{:}, options);
          else
              response = webwrite(url, data, options);
          end
      catch ME
          error('API request failed: %s', ME.message);
      end
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

    function dataset_id = find_dataset_id(obj, dataset_path)
      endpoint = sprintf('/datapool/%s/dataset', obj.datapool);
      res = obj.make_request('GET', endpoint, [], struct('path', dataset_path));
      dataset_id = res.id;
    end

    function signal_id = find_signal_id(obj, dataset_id, signal_name)
      endpoint = sprintf('/datapool/%s/dataset/%d/signal', obj.datapool, dataset_id);
      res = obj.make_request('GET', endpoint, [], struct('name', signal_name));
      signal_id = res.id;
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
      dataset_id = obj.find_dataset_id(dataset_path);
      signal_id = obj.find_signal_id(dataset_id, signal_name);
      cache = sprintf('_marplecache/%s/%s/dataset=%d/signal=%d', obj.workspace, obj.datapool, dataset_id, signal_id);

      if ~isfolder(cache)
        mkdir(cache)
        endpoint = sprintf('/datapool/%s/dataset/%d/signal/%d/data', obj.datapool, dataset_id, signal_id);
        paths = obj.make_request('GET', endpoint);
        for i = 1:length(paths)
          url = paths{i};
          [~, name, ext] = fileparts(extractBefore(url, '?'));
          parquet_name = [name ext];
          cache_path = sprintf('%s/%s', cache, parquet_name);
          websave(cache_path, url);
        end
      end

      try
        ds = parquetDatastore(cache);
        T = readall(ds);
      catch
        DB.transcode_cache(cache);
        ds = parquetDatastore(cache);
        T = readall(ds);
      end

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
