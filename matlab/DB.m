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
    TRANSCODE_VERSION = 'v0.2.0'
    TRANSCODE_BASE_URL = 'https://github.com/marpledata/marple-sdk/releases/download/parquet-transcode'
    MAX_ROWS_PER_FILE = 16 * 1048576
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
        case 'glnxa64'
          platform = 'linux-x64';
          ext = '';
        otherwise
          error('Unsupported platform: %s', arch);
      end

      bin_name = sprintf('parquet-transcode-%s-%s%s', DB.TRANSCODE_VERSION, platform, ext);
      bin_dir = fullfile(fileparts(mfilename('fullpath')), '_marplecache');
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

    function meta = prepare_upload_file(input_path, output_path, dataset_id, signal_id, expected_rows)
      bin_path = DB.ensure_binary();
      stderr_path = [tempname '.err'];
      cmd = sprintf( ...
        '"%s" prepare-upload --input "%s" --output "%s" --dataset-id %d --signal-id %d --expected-rows %d 2>"%s"', ...
        bin_path, input_path, output_path, dataset_id, signal_id, expected_rows, stderr_path);
      [status, out] = system(cmd);
      err_txt = '';
      if isfile(stderr_path)
        try
          err_txt = fileread(stderr_path);
        catch
        end
        try
          delete(stderr_path);
        catch
        end
      end
      if status ~= 0
        detail = strtrim(out);
        if ~isempty(strtrim(err_txt))
          detail = strtrim(sprintf('%s\n%s', detail, err_txt));
        end
        error('parquet-transcode prepare-upload failed: %s', detail);
      end
      try
        meta = jsondecode(strtrim(out));
      catch ME
        error('Failed to parse prepare-upload JSON from stdout: %s\nOutput was: %s\nStderr: %s', ...
          ME.message, out, err_txt);
      end
      if ~isfield(meta, 'rows') || ~isfield(meta, 'size') || ~isfield(meta, 'footer')
        error('prepare-upload JSON missing rows/size/footer: %s', out);
      end
      if double(meta.rows) ~= double(expected_rows)
        error('prepare-upload row count %d does not match expected %d', ...
          meta.rows, expected_rows);
      end
    end

    function time = datetime_to_unix_ns(row_times, name)
      if isduration(row_times)
        error('%s: timetable RowTimes must be datetime, not duration', name);
      end
      if ~isdatetime(row_times)
        error('%s: timetable RowTimes must be datetime', name);
      end
      if any(isnat(row_times))
        error('%s: timetable RowTimes must not contain NaT', name);
      end

      row_times.TimeZone = 'UTC';
      epoch = datetime(1970, 1, 1, 0, 0, 0, 'TimeZone', 'UTC');
      seconds_since_epoch = seconds(row_times - epoch);
      if any(seconds_since_epoch < 0)
        error('%s: timetable RowTimes must be on or after the Unix epoch', name);
      end
      if any(seconds_since_epoch >= double(intmax('int64')) / 1e9)
        error('%s: timetable RowTimes exceed the int64-nanosecond range', name);
      end
      time = int64(round(seconds_since_epoch * 1e9));
    end

    function time = validate_numeric_time(raw_time, name)
      if ~isnumeric(raw_time) || ~isreal(raw_time)
        error( ...
          ['%s: table variable ''time'' must be a real numeric column of ', ...
           'int64 Unix nanoseconds; use a timetable for datetime values'], name);
      end
      if ~iscolumn(raw_time)
        error('%s: table variable ''time'' must contain exactly one column', name);
      end
      if isfloat(raw_time) && any(~isfinite(raw_time))
        error('%s: table variable ''time'' must not contain NaN or Inf', name);
      end
      if any(raw_time < 0)
        error('%s: table variable ''time'' must be greater than or equal to 0', name);
      end
      if isfloat(raw_time)
        if any(raw_time ~= fix(raw_time))
          error('%s: table variable ''time'' must contain integer nanoseconds', name);
        end
        if any(raw_time > flintmax(class(raw_time)))
          error( ...
            ['%s: floating-point ''time'' values are too large to represent ', ...
             'nanoseconds exactly; convert them to int64 first'], name);
        end
      elseif isa(raw_time, 'uint64') && any(raw_time > uint64(intmax('int64')))
        error('%s: table variable ''time'' exceeds the int64-nanosecond range', name);
      end
      time = int64(raw_time);
    end

    function T = normalize_signal_table(data, name)
      if ~istimetable(data) && ~istable(data)
        error('%s: data must be a table or timetable', name);
      end
      if height(data) < 1
        error('%s: Signal must have at least one row', name);
      end

      if istimetable(data)
        row_times = data.Properties.RowTimes;
        vars = data.Properties.VariableNames;
        if ismember('time', vars)
          error( ...
            ['%s: timetable data must use RowTimes for timestamps; ', ...
             'remove the variable named ''time'''], name);
        end
        time = DB.datetime_to_unix_ns(row_times, name);
        T = table(time, 'VariableNames', {'time'});
        for i = 1:numel(vars)
          T.(vars{i}) = data.(vars{i});
        end
      else
        T = data;
      end

      vars = T.Properties.VariableNames;
      if ~ismember('time', vars)
        error('%s: Data must include a ''time'' column', name);
      end
      if ~ismember('value', vars) && ~ismember('value_text', vars)
        error('%s: Data must include ''value'' and/or ''value_text''', name);
      end

      time = DB.validate_numeric_time(T.time, name);

      n = height(T);
      if ismember('value', vars)
        try
          value = double(T.value);
        catch ME
          error('%s: ''value'' must be float64-compatible: %s', name, ME.message);
        end
        value(~isfinite(value)) = NaN;
      else
        value = NaN(n, 1);
      end

      if ismember('value_text', vars)
        try
          value_text = string(T.value_text);
        catch ME
          error('%s: ''value_text'' must be string-compatible: %s', name, ME.message);
        end
      else
        value_text = strings(n, 1);
        value_text(:) = missing;
      end

      T = table(time, value, value_text, 'VariableNames', {'time', 'value', 'value_text'});
    end

    function row_counts = plan_row_counts(num_rows)
      full = floor(num_rows / DB.MAX_ROWS_PER_FILE);
      remainder = mod(num_rows, DB.MAX_ROWS_PER_FILE);
      row_counts = repmat(DB.MAX_ROWS_PER_FILE, 1, full);
      if remainder > 0
        row_counts(end+1) = remainder; %#ok<AGROW>
      end
    end

    function frequency = estimate_frequency(time)
      frequency = [];
      if numel(time) < 2
        return;
      end
      % Keep int64 diffs — double(time) loses ~256 ns near Unix-ns epoch values.
      diffs = diff(time);
      diffs = diffs(diffs > 0);
      if isempty(diffs)
        return;
      end
      frequency = 1e9 / double(median(diffs));
    end

    function s = value_sum(value)
      finite = value(isfinite(value));
      if isempty(finite)
        s = [];
      else
        s = sum(finite);
      end
    end

    function json = encode_json_object(s)
      % jsonencode(struct()) yields [] on many MATLAB releases; force {}.
      if isempty(s) || (isstruct(s) && isempty(fieldnames(s)))
        json = '{}';
      else
        json = jsonencode(s);
      end
    end

    function json = encode_json_nullable_number(x)
      if isempty(x) || (isnumeric(x) && ~isfinite(x))
        json = 'null';
      else
        json = jsonencode(x);
      end
    end

    function put_storage_file(local_path, put_url)
      import matlab.net.http.*
      import matlab.net.http.io.*
      import matlab.net.URI

      % Prefer FileProvider streaming; strip Content-Disposition which breaks
      % many presigned object-storage URLs. Fall back to raw uint8 Payload when
      % FileProvider/complete fails (not when the HTTP status is non-2xx).
      uri = URI(put_url, 'literal'); % avoid re-encoding '%' in the presigned path
      try
        provider = FileProvider(local_path);
        header = HeaderField('Content-Type', 'application/octet-stream');
        req = RequestMessage(RequestMethod.PUT, header, provider);
        [completed, ~] = complete(req, uri);
        if ~isempty(completed.Header.getFields('Content-Disposition'))
          completed.Header = completed.Header.removeFields('Content-Disposition');
        end
        % Re-assert Content-Type in case complete replaced it from the file suffix.
        completed.Header = replaceFields(completed.Header, ...
          HeaderField('Content-Type', 'application/octet-stream'));
        resp = send(completed, uri);
      catch
        fid = fopen(local_path, 'rb');
        if fid < 0
          error('Storage PUT failed: could not open %s', local_path);
        end
        cleaner = onCleanup(@() fclose(fid));
        bytes = fread(fid, Inf, '*uint8');
        clear cleaner;
        body = MessageBody();
        body.Payload = bytes;
        header = HeaderField('Content-Type', 'application/octet-stream');
        req = RequestMessage(RequestMethod.PUT, header, body);
        resp = req.send(uri);
      end

      code = double(resp.StatusCode);
      if code < 200 || code >= 300
        reason = '';
        try
          reason = char(resp.StatusLine.ReasonPhrase);
        catch
        end
        error('Storage PUT failed: HTTP %d %s', code, reason);
      end
    end

    function delete_temp_dir(temp_dir)
      if isfolder(temp_dir)
        try
          rmdir(temp_dir, 's');
        catch
        end
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

    function response = post_json(obj, endpoint, json_body)
      import matlab.net.http.*
      import matlab.net.URI

      headers = [
        HeaderField('Authorization', ['Bearer ' obj.api_key]), ...
        HeaderField('X-Request-Source', 'sdk/matlab'), ...
        HeaderField('Content-Type', 'application/json')
      ];
      body = MessageBody();
      body.Payload = unicode2native(char(json_body), 'UTF-8');
      req = RequestMessage(RequestMethod.POST, headers, body);
      url = [obj.api_url endpoint];
      try
        resp = req.send(URI(url));
      catch ME
        error('API request failed: %s', ME.message);
      end

      code = double(resp.StatusCode);
      raw = '';
      try
        if ~isempty(resp.Body) && ~isempty(resp.Body.Payload)
          raw = native2unicode(resp.Body.Payload(:)', 'UTF-8');
        elseif ~isempty(resp.Body) && ~isempty(resp.Body.Data)
          if ischar(resp.Body.Data) || isstring(resp.Body.Data)
            raw = char(resp.Body.Data);
          else
            response = resp.Body.Data;
            if code < 200 || code >= 300
              error('API request failed: HTTP %d', code);
            end
            return;
          end
        end
      catch
      end

      if code < 200 || code >= 300
        detail = strtrim(raw);
        if isempty(detail)
          error('API request failed: HTTP %d', code);
        end
        error('API request failed: HTTP %d: %s', code, detail);
      end

      if isempty(strtrim(raw))
        response = [];
        return;
      end
      try
        response = jsondecode(raw);
      catch ME
        error('API request failed: could not decode JSON response: %s', ME.message);
      end
    end

    function stream_id = find_stream_id(obj, stream_name)
      for i = 1:length(obj.streams)
        if strcmpi(obj.streams{i}.name, stream_name)
          stream_id = obj.streams{i}.id;
          return;
        end
      end

      % If we get here, stream wasn't found
      available_names = strjoin(cellfun(@(s) s.name, obj.streams, 'UniformOutput', false), ', ');
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

    function cache = signal_cache_path(obj, dataset_id, signal_id)
      cache = fullfile( ...
        '_marplecache', char(string(obj.workspace)), char(string(obj.datapool)), ...
        sprintf('dataset=%d', dataset_id), sprintf('signal=%d', signal_id));
    end

    function clear_signal_cache(obj, dataset_id, signal_id)
      cache = obj.signal_cache_path(dataset_id, signal_id);
      if isfolder(cache)
        try
          rmdir(cache, 's');
        catch ME
          error( ...
            ['Signal upload completed, but failed to clear cached data for ', ...
             'dataset %d signal %d: %s'], ...
            dataset_id, signal_id, ME.message);
        end
      end
    end

    function dataset = find_dataset(obj, stream_name, dataset_id)
      datasets = obj.get_datasets(stream_name);
      if iscell(datasets)
        for i = 1:numel(datasets)
          if double(datasets{i}.id) == double(dataset_id)
            dataset = datasets{i};
            return;
          end
        end
      else
        for i = 1:numel(datasets)
          if double(datasets(i).id) == double(dataset_id)
            dataset = datasets(i);
            return;
          end
        end
      end
      error('Dataset id %d not found in stream "%s"', dataset_id, stream_name);
    end

    function assert_time_overlap(~, name, time, dataset)
      has_start = isfield(dataset, 'timestamp_start') && ~isempty(dataset.timestamp_start);
      has_stop = isfield(dataset, 'timestamp_stop') && ~isempty(dataset.timestamp_stop);
      if ~(has_start && has_stop)
        return;
      end
      time_min = min(time);
      time_max = max(time);
      ds_start = int64(dataset.timestamp_start);
      ds_stop = int64(dataset.timestamp_stop);
      if time_max < ds_start || time_min > ds_stop
        error( ...
          '%s: Signal time range [%d, %d] does not overlap dataset range [%d, %d]', ...
          name, time_min, time_max, ds_start, ds_stop);
      end
    end

    function staging_paths = write_staging_files(~, T, row_counts, temp_dir)
      staging_paths = cell(1, numel(row_counts));
      offset = 0;
      for i = 1:numel(row_counts)
        rows = row_counts(i);
        part = T(offset+1:offset+rows, :);
        path = fullfile(temp_dir, sprintf('staging_%d.parquet', i-1));
        % Snappy staging only; lake format is owned by parquet-transcode.
        parquetwrite(path, part, 'VariableCompression', 'snappy');
        staging_paths{i} = path;
        offset = offset + rows;
      end
    end

    function format_upload_statuses(~, signals)
      parts = {};
      if iscell(signals)
        items = signals;
      elseif isstruct(signals)
        items = num2cell(signals);
      else
        return;
      end
      for i = 1:numel(items)
        s = items{i};
        label = '';
        if isfield(s, 'name') && ~isempty(s.name)
          label = char(string(s.name));
        elseif isfield(s, 'id') && ~isempty(s.id)
          label = sprintf('id=%s', string(s.id));
        else
          label = sprintf('signal[%d]', i);
        end
        status = '';
        if isfield(s, 'status')
          status = char(string(s.status));
        end
        message = '';
        if isfield(s, 'message') && ~isempty(s.message)
          message = char(string(s.message));
        end
        if isempty(message)
          parts{end+1} = sprintf('%s: %s', label, status); %#ok<AGROW>
        else
          parts{end+1} = sprintf('%s: %s (%s)', label, status, message); %#ok<AGROW>
        end
      end
      if ~isempty(parts)
        error('Signal upload failed: %s', strjoin(parts, '; '));
      end
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
      streams = response.streams;
      % jsondecode returns a struct array when all stream objects have
      % identical fields, but a cell array when they don't. Normalize to
      % a cell array so consumers only have to handle one shape.
      if isstruct(streams)
        streams = num2cell(streams);
      end
      obj.streams = streams;
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

    function dataset = add_dataset(obj, stream_name, dataset_name, opts)
      %ADD_DATASET Create a new empty dataset in a stream.
      %
      %   dataset = mdb.add_dataset(stream_name, dataset_name)
      %   dataset = mdb.add_dataset(stream_name, dataset_name, Metadata=struct())
      %
      %   Live stream: append data, then cool to cold Parquet/Iceberg storage.
      %   File stream: prefer file upload; use add_dataset + add_signal for
      %   custom lake writes without file parsing.
      arguments
        obj
        stream_name
        dataset_name
        opts.Metadata = struct()
      end

      dataset_name = char(string(dataset_name));
      if strlength(strtrim(string(dataset_name))) == 0
        error('Dataset name must be non-empty');
      end
      stream_name = char(string(stream_name));

      if isempty(obj.streams)
        obj.streams = obj.get_streams();
      end
      stream_id = obj.find_stream_id(stream_name);

      body = sprintf('{"dataset_name":%s,"metadata":%s}', ...
        jsonencode(dataset_name), DB.encode_json_object(opts.Metadata));

      endpoint = sprintf('/stream/%d/dataset/add', stream_id);
      try
        resp = obj.post_json(endpoint, body);
      catch ME
        error('Add dataset failed for "%s": %s', dataset_name, ME.message);
      end

      if ~isfield(resp, 'dataset_id')
        error('Add dataset response missing dataset_id for "%s"', dataset_name);
      end
      dataset_id = double(resp.dataset_id);

      endpoint_get = sprintf('/datapool/%s/dataset', obj.datapool);
      try
        dataset = obj.make_request('GET', endpoint_get, [], struct('id', dataset_id));
      catch ME
        error('Failed to fetch dataset "%s" after creation (id=%d): %s', ...
          dataset_name, dataset_id, ME.message);
      end
    end

    function dataset = update_metadata(obj, stream_name, dataset_id, metadata)
      %UPDATE_METADATA Update the metadata of an existing dataset.
      %
      %   dataset = mdb.update_metadata(stream_name, dataset_id, metadata)
      %
      %   `metadata` is merged into the dataset's existing metadata
      %   server-side (matching keys are overwritten; keys already on the
      %   dataset but absent from `metadata` are left untouched). Returns the
      %   refreshed dataset.
      arguments
        obj
        stream_name
        dataset_id (1,1) double
        metadata
      end

      stream_name = char(string(stream_name));
      if isempty(obj.streams)
        obj.streams = obj.get_streams();
      end
      stream_id = obj.find_stream_id(stream_name);

      endpoint = sprintf('/stream/%d/dataset/%d/metadata', stream_id, dataset_id);
      try
        obj.post_json(endpoint, DB.encode_json_object(metadata));
      catch ME
        error('Update metadata failed for dataset %d: %s', dataset_id, ME.message);
      end

      endpoint_get = sprintf('/datapool/%s/dataset', obj.datapool);
      try
        dataset = obj.make_request('GET', endpoint_get, [], struct('id', dataset_id));
      catch ME
        error('Failed to fetch dataset %d after metadata update: %s', dataset_id, ME.message);
      end
    end

    function signal = add_signal(obj, stream_name, dataset_id, name, data, opts)
      %ADD_SIGNAL Upload one signal onto an existing imported dataset.
      %
      %   signal = mdb.add_signal(stream_name, dataset_id, name, data)
      %   signal = mdb.add_signal(..., Metadata=struct(), Overwrite=false, Priority="default")
      %
      %   A table uses int64 Unix nanoseconds in time. A timetable uses datetime
      %   RowTimes. Both need value and/or value_text data.
      %   Returns the signal object after upload completion is accepted; the
      %   asynchronous Iceberg commit may still be in progress.
      arguments
        obj
        stream_name
        dataset_id (1,1) double
        name
        data
        opts.Metadata = struct()
        opts.Overwrite (1,1) logical = false
        opts.Priority (1,1) string {mustBeMember(opts.Priority, ["default","high"])} = "default"
      end

      name = char(string(name));
      if strlength(strtrim(string(name))) == 0
        error('Signal name must be non-empty');
      end
      stream_name = char(string(stream_name));
      priority = char(opts.Priority);

      if isempty(obj.streams)
        obj.streams = obj.get_streams();
      end
      stream_id = obj.find_stream_id(stream_name);
      dataset = obj.find_dataset(stream_name, dataset_id);

      T = DB.normalize_signal_table(data, name);
      obj.assert_time_overlap(name, T.time, dataset);

      row_counts = DB.plan_row_counts(height(T));
      frequency = DB.estimate_frequency(T.time);
      sum_value = DB.value_sum(T.value);

      temp_dir = tempname;
      mkdir(temp_dir);
      cleaner = onCleanup(@() DB.delete_temp_dir(temp_dir)); %#ok<NASGU>

      staging_paths = obj.write_staging_files(T, row_counts, temp_dir);

      files_json_parts = cell(1, numel(row_counts));
      for i = 1:numel(row_counts)
        files_json_parts{i} = sprintf('{"index":%d,"rows":%d}', i-1, row_counts(i));
      end
      overwrite_json = 'false';
      if opts.Overwrite
        overwrite_json = 'true';
      end
      presign_body = sprintf( ...
        '{"signals":[{"name":%s,"metadata":%s,"files":[%s],"priority":%s}],"overwrite":%s}', ...
        jsonencode(name), ...
        DB.encode_json_object(opts.Metadata), ...
        strjoin(files_json_parts, ','), ...
        jsonencode(priority), ...
        overwrite_json);

      endpoint_presign = sprintf('/stream/%d/dataset/%d/signal/uploads', stream_id, dataset_id);
      try
        presign_resp = obj.post_json(endpoint_presign, presign_body);
      catch ME
        error('Presign signal upload failed for "%s": %s', name, ME.message);
      end

      if iscell(presign_resp)
        presigned_list = presign_resp;
      elseif isstruct(presign_resp)
        presigned_list = num2cell(presign_resp);
      else
        error('Unexpected presign response for "%s"', name);
      end

      presigned = [];
      for i = 1:numel(presigned_list)
        item = presigned_list{i};
        if isfield(item, 'name') && strcmp(char(string(item.name)), name)
          presigned = item;
          break;
        end
      end
      if isempty(presigned)
        error('Presign response missing signal "%s"', name);
      end
      signal_id = double(presigned.signal_id);

      if iscell(presigned.files)
        file_list = presigned.files;
      else
        file_list = num2cell(presigned.files);
      end
      files_by_index = containers.Map('KeyType', 'double', 'ValueType', 'any');
      for i = 1:numel(file_list)
        f = file_list{i};
        files_by_index(double(f.index)) = f;
      end

      uploaded_files = cell(1, numel(row_counts));
      try
        for i = 1:numel(row_counts)
          idx = i - 1;
          if ~isKey(files_by_index, idx)
            error('Presign response missing file index %d for "%s"', idx, name);
          end
          remote = files_by_index(idx);
          upload_path = fullfile(temp_dir, sprintf('upload_%d.parquet', idx));
          meta = DB.prepare_upload_file( ...
            staging_paths{i}, upload_path, dataset_id, signal_id, row_counts(i));
          DB.put_storage_file(upload_path, char(string(remote.url)));
          uploaded_files{i} = struct( ...
            'path', char(string(remote.path)), ...
            'size', double(meta.size), ...
            'footer', double(meta.footer));
        end
      catch ME
        error( ...
          ['Signal upload failed after presign for "%s" (signal_id=%d). ', ...
           'Complete was not called; a FROZEN_TO_COLD placeholder may remain server-side. ', ...
           'Cause: %s'], name, signal_id, ME.message);
      end

      file_complete_parts = cell(1, numel(uploaded_files));
      for i = 1:numel(uploaded_files)
        f = uploaded_files{i};
        file_complete_parts{i} = sprintf( ...
          '{"path":%s,"size":%d,"footer":%d}', ...
          jsonencode(f.path), f.size, f.footer);
      end
      complete_body = sprintf( ...
        '{"signals":[{"id":%d,"priority":%s,"stats":{"sum":%s,"frequency":%s},"files":[%s]}]}', ...
        signal_id, ...
        jsonencode(priority), ...
        DB.encode_json_nullable_number(sum_value), ...
        DB.encode_json_nullable_number(frequency), ...
        strjoin(file_complete_parts, ','));

      endpoint_complete = sprintf( ...
        '/stream/%d/dataset/%d/signal/uploads/complete', stream_id, dataset_id);
      try
        complete_resp = obj.post_json(endpoint_complete, complete_body);
      catch ME
        error('Complete signal upload failed for "%s" (id=%d): %s', name, signal_id, ME.message);
      end

      if iscell(complete_resp)
        statuses = complete_resp;
      elseif isstruct(complete_resp)
        statuses = num2cell(complete_resp);
      else
        statuses = {complete_resp};
      end
      if isempty(statuses)
        error('Complete signal upload returned empty status list for id=%d', signal_id);
      end
      for i = 1:numel(statuses)
        st = statuses{i};
        status_ok = isfield(st, 'status') && strcmp(char(string(st.status)), 'OK');
        if ~status_ok
          obj.format_upload_statuses(statuses);
        end
      end
      obj.clear_signal_cache(dataset_id, signal_id);

      endpoint_get = sprintf( ...
        '/stream/%d/dataset/%d/signal/%d', stream_id, dataset_id, signal_id);
      try
        signal = obj.make_request('GET', endpoint_get);
      catch ME
        error('Failed to fetch signal "%s" after upload (id=%d): %s', ...
          name, signal_id, ME.message);
      end
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
      cache = obj.signal_cache_path(dataset_id, signal_id);

      if ~isfolder(cache) || isempty(dir(fullfile(cache, '*.parquet')))
        if ~isfolder(cache)
          mkdir(cache)
        end
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
