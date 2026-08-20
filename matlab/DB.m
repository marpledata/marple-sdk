% DB.m
classdef DB
  properties
    api_url
    api_key
    workspace
    datapool
    streams % Cache for available streams
  end

  properties (Constant)
    VERSION = "0.3.0"
  end

  properties (Constant, Access = private)
    TRANSCODE_VERSION = 'v0.2.0'
    TRANSCODE_BASE_URL = 'https://github.com/marpledata/marple-sdk/releases/download/parquet-transcode'
    MAX_ROWS_PER_FILE = 16 * 1048576
  end

  methods (Static, Access = private)
    function src = request_source()
      src = sprintf('sdk/matlab:%s', DB.VERSION);
    end

    function must(cond, msg, varargin)
      if ~cond
        error(msg, varargin{:});
      end
    end

    function s = as_char(x)
      s = char(string(x));
    end

    function s = require_name(x, what)
      s = DB.as_char(x);
      if isempty(strtrim(s))
        error('%s must be non-empty', what);
      end
    end

    function c = as_cell(x)
      if iscell(x)
        c = x;
      elseif isstruct(x)
        c = num2cell(x);
      elseif isempty(x)
        c = {};
      else
        c = {x};
      end
    end

    function item = find_by_name(list, name, ignore_case)
      items = DB.as_cell(list);
      name = DB.as_char(name);
      item = [];
      for i = 1:numel(items)
        if ~isfield(items{i}, 'name')
          continue;
        end
        item_name = DB.as_char(items{i}.name);
        if ignore_case
          matched = strcmpi(item_name, name);
        else
          matched = strcmp(item_name, name);
        end
        if matched
          item = items{i};
          return;
        end
      end
    end

    function item = find_by_id(list, id)
      items = DB.as_cell(list);
      item = [];
      for i = 1:numel(items)
        if isfield(items{i}, 'id') && double(items{i}.id) == double(id)
          item = items{i};
          return;
        end
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

    function text = read_and_delete(path)
      text = '';
      if ~isfile(path)
        return;
      end
      try
        text = fileread(path);
      catch
      end
      try
        delete(path);
      catch
      end
    end

    function cfg = read_config()
      json_path = fullfile(fileparts(mfilename('fullpath')), 'config.json');
      DB.must(isfile(json_path), 'Configuration file not found: %s', json_path);
      cfg = jsondecode(fileread(json_path));
    end

    function bin_path = ensure_binary()
      arch = computer('arch');
      ext = '';
      switch arch
        case 'win64'
          platform = 'windows-x64';
          ext = '.exe';
        case 'maca64'
          platform = 'darwin-arm64';
        case 'glnxa64'
          platform = 'linux-x64';
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
      DB.must(status == 0, 'Parquet transcode failed: %s', msg);
    end

    function meta = prepare_upload_file(input_path, output_path, dataset_id, signal_id, expected_rows)
      bin_path = DB.ensure_binary();
      stderr_path = [tempname '.err'];
      cmd = sprintf( ...
        '"%s" prepare-upload --input "%s" --output "%s" --dataset-id %d --signal-id %d --expected-rows %d 2>"%s"', ...
        bin_path, input_path, output_path, dataset_id, signal_id, expected_rows, stderr_path);
      [status, out] = system(cmd);
      err_txt = DB.read_and_delete(stderr_path);
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
      DB.must(isfield(meta, 'rows') && isfield(meta, 'size') && isfield(meta, 'footer'), ...
        'prepare-upload JSON missing rows/size/footer: %s', out);
      DB.must(double(meta.rows) == double(expected_rows), ...
        'prepare-upload row count %d does not match expected %d', meta.rows, expected_rows);
    end

    function time = datetime_to_unix_ns(row_times, name)
      DB.must(~isduration(row_times), '%s: timetable RowTimes must be datetime, not duration', name);
      DB.must(isdatetime(row_times), '%s: timetable RowTimes must be datetime', name);
      DB.must(~any(isnat(row_times)), '%s: timetable RowTimes must not contain NaT', name);

      row_times.TimeZone = 'UTC';
      epoch = datetime(1970, 1, 1, 0, 0, 0, 'TimeZone', 'UTC');
      seconds_since_epoch = seconds(row_times - epoch);
      DB.must(~any(seconds_since_epoch < 0), ...
        '%s: timetable RowTimes must be on or after the Unix epoch', name);
      DB.must(~any(seconds_since_epoch >= double(intmax('int64')) / 1e9), ...
        '%s: timetable RowTimes exceed the int64-nanosecond range', name);
      time = int64(round(seconds_since_epoch * 1e9));
    end

    function time = validate_numeric_time(raw_time, name)
      DB.must(isnumeric(raw_time) && isreal(raw_time), ...
        ['%s: table variable ''time'' must be a real numeric column of ', ...
         'int64 Unix nanoseconds; use a timetable for datetime values'], name);
      DB.must(iscolumn(raw_time), '%s: table variable ''time'' must contain exactly one column', name);
      DB.must(~(isfloat(raw_time) && any(~isfinite(raw_time))), ...
        '%s: table variable ''time'' must not contain NaN or Inf', name);
      DB.must(~any(raw_time < 0), '%s: table variable ''time'' must be greater than or equal to 0', name);
      if isfloat(raw_time)
        DB.must(~any(raw_time ~= fix(raw_time)), ...
          '%s: table variable ''time'' must contain integer nanoseconds', name);
        DB.must(~any(raw_time > flintmax(class(raw_time))), ...
          ['%s: floating-point ''time'' values are too large to represent ', ...
           'nanoseconds exactly; convert them to int64 first'], name);
      else
        DB.must(~(isa(raw_time, 'uint64') && any(raw_time > uint64(intmax('int64')))), ...
          '%s: table variable ''time'' exceeds the int64-nanosecond range', name);
      end
      time = int64(raw_time);
    end

    function T = normalize_signal_table(data, name)
      DB.must(istimetable(data) || istable(data), '%s: data must be a table or timetable', name);
      DB.must(height(data) >= 1, '%s: Signal must have at least one row', name);

      if istimetable(data)
        row_times = data.Properties.RowTimes;
        vars = data.Properties.VariableNames;
        DB.must(~ismember('time', vars), ...
          ['%s: timetable data must use RowTimes for timestamps; ', ...
           'remove the variable named ''time'''], name);
        time = DB.datetime_to_unix_ns(row_times, name);
        T = table(time, 'VariableNames', {'time'});
        for i = 1:numel(vars)
          T.(vars{i}) = data.(vars{i});
        end
      else
        T = data;
      end

      vars = T.Properties.VariableNames;
      DB.must(ismember('time', vars), '%s: Data must include a ''time'' column', name);
      DB.must(ismember('value', vars) || ismember('value_text', vars), ...
        '%s: Data must include ''value'' and/or ''value_text''', name);

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

    function send_file(local_path, url, method, opts)
      % Stream a local file with FileProvider.send (no complete()).
      % Multipart writes a one-part form to a temp file first.
      arguments
        local_path
        url
        method
        opts.Bearer = ""
        opts.Multipart (1,1) logical = false
      end
      import matlab.net.http.*
      import matlab.net.http.io.*
      import matlab.net.URI

      headers = HeaderField.empty;
      if strlength(string(opts.Bearer)) > 0
        headers = [
          HeaderField('Authorization', ['Bearer ' DB.as_char(opts.Bearer)]), ...
          HeaderField('X-Request-Source', DB.request_source())
        ];
      end
      local_path = DB.as_char(local_path);
      send_path = local_path;
      content_type = 'application/octet-stream';
      uri = URI(url, 'literal');
      try
        if opts.Multipart
          [~, name, ext] = fileparts(local_path);
          tmp = [tempname '.upload'];
          cleaner = onCleanup(@() delete(tmp)); %#ok<NASGU>
          boundary = DB.write_multipart_upload(local_path, tmp, [name ext]);
          send_path = tmp;
          content_type = ['multipart/form-data; boundary=' boundary];
        end
        % Empty Content-Disposition suppresses FileProvider's attachment header.
        headers = [headers, ...
          HeaderField('Content-Type', content_type), ...
          HeaderField('Content-Disposition')];
        provider = FileProvider(send_path);
        req = RequestMessage(method, headers, provider);
        resp = send(req, uri);
      catch ME
        err = MException('Marple:FileUploadFailed', 'File upload failed: %s', ME.message);
        throw(err.addCause(ME));
      end
      code = double(resp.StatusCode);
      DB.must(code >= 200 && code < 300, 'File upload failed: HTTP %d: %s', code, DB.response_text(resp));
    end

    function boundary = write_multipart_upload(src_path, dest_path, filename)
      filename = regexprep(DB.as_char(filename), '[\r\n"]', '');
      boundary = sprintf('----MarpleBoundary%08X%08X', randi(2^31-1), randi(2^31-1));
      preamble = unicode2native(sprintf([ ...
        '--%s\r\n' ...
        'Content-Disposition: form-data; name="file"; filename="%s"\r\n' ...
        'Content-Type: application/octet-stream\r\n' ...
        '\r\n'], boundary, filename), 'UTF-8');
      epilogue = unicode2native(sprintf('\r\n--%s--\r\n', boundary), 'UTF-8');

      out = fopen(dest_path, 'wb');
      DB.must(out >= 0, 'Could not create upload temp file: %s', dest_path);
      cleaner_out = onCleanup(@() fclose(out)); %#ok<NASGU>
      fwrite(out, preamble, 'uint8');

      in = fopen(src_path, 'rb');
      DB.must(in >= 0, 'Could not open file for upload: %s', src_path);
      cleaner_in = onCleanup(@() fclose(in)); %#ok<NASGU>
      while true
        chunk = fread(in, 65536, '*uint8');
        if isempty(chunk)
          break;
        end
        fwrite(out, chunk, 'uint8');
      end
      fwrite(out, epilogue, 'uint8');
    end

    function text = response_text(resp)
      text = '';
      try
        if ~isempty(resp.Body.Payload)
          text = strtrim(native2unicode(resp.Body.Payload(:)', 'UTF-8'));
        end
      catch
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
      cfg = DB.read_config();
      obj = DB(cfg.api_url, cfg.api_key);
      obj.workspace = cfg.workspace;
      if isfield(cfg, 'datapool')
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
        'X-Request-Source', DB.request_source()
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
        HeaderField('X-Request-Source', DB.request_source()), ...
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
            if code >= 200 && code < 300
              return;
            end
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

    function response = post_or_error(obj, endpoint, json_body, fmt, varargin)
      try
        response = obj.post_json(endpoint, json_body);
      catch ME
        error(fmt, varargin{:}, ME.message);
      end
    end

    function dataset = fetch_dataset(obj, dataset_id, fmt, varargin)
      try
        dataset = obj.get_dataset(struct('id', dataset_id));
      catch ME
        error(fmt, varargin{:}, ME.message);
      end
    end

    function stream_id = find_stream_id(obj, stream_name)
      % Retry once against a fresh list -- obj.streams is a per-call snapshot
      % (DB isn't a handle class), so a stream created earlier in the same
      % session via create_stream won't be in the caller's cached copy yet.
      for attempt = 1:2
        found = DB.find_by_name(obj.streams, stream_name, true);
        if ~isempty(found)
          stream_id = found.id;
          return;
        end
        if attempt == 1
          obj.streams = obj.get_streams();
        end
      end

      available_names = strjoin(cellfun(@(s) s.name, obj.streams, 'UniformOutput', false), ', ');
      error('Stream "%s" not found. Available streams are: %s', stream_name, available_names);
    end

    function dataset = get_dataset(obj, query)
      endpoint = sprintf('/datapool/%s/dataset', obj.datapool);
      dataset = obj.make_request('GET', endpoint, [], query);
    end

    function signal_id = find_signal_id(obj, dataset_id, signal_name)
      endpoint = sprintf('/datapool/%s/dataset/%d/signal', obj.datapool, dataset_id);
      res = obj.make_request('GET', endpoint, [], struct('name', signal_name));
      signal_id = res.id;
    end

    function cache = workspace_cache(obj)
      cache = fullfile('_marplecache', DB.as_char(obj.workspace), DB.as_char(obj.datapool));
    end

    function cache = signal_cache_path(obj, dataset_id, signal_id)
      cache = fullfile(obj.workspace_cache(), ...
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
      dataset = DB.find_by_id(obj.get_datasets(stream_name), dataset_id);
      DB.must(~isempty(dataset), 'Dataset id %d not found in stream "%s"', dataset_id, stream_name);
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
      DB.must(time_max >= ds_start && time_min <= ds_stop, ...
        '%s: Signal time range [%d, %d] does not overlap dataset range [%d, %d]', ...
        name, time_min, time_max, ds_start, ds_stop);
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
      items = DB.as_cell(signals);
      parts = cell(1, numel(items));
      for i = 1:numel(items)
        s = items{i};
        if isfield(s, 'name') && ~isempty(s.name)
          label = DB.as_char(s.name);
        elseif isfield(s, 'id') && ~isempty(s.id)
          label = sprintf('id=%s', string(s.id));
        else
          label = sprintf('signal[%d]', i);
        end
        status = '';
        if isfield(s, 'status')
          status = DB.as_char(s.status);
        end
        if isfield(s, 'message') && ~isempty(s.message)
          parts{i} = sprintf('%s: %s (%s)', label, status, DB.as_char(s.message));
        else
          parts{i} = sprintf('%s: %s', label, status);
        end
      end
      if isempty(parts)
        error('Signal upload failed');
      end
      error('Signal upload failed: %s', strjoin(parts, '; '));
    end

    function presigned = presign_signal_upload(obj, stream_id, dataset_id, name, row_counts, metadata, priority, overwrite)
      files_json_parts = cell(1, numel(row_counts));
      for i = 1:numel(row_counts)
        files_json_parts{i} = sprintf('{"index":%d,"rows":%d}', i-1, row_counts(i));
      end
      body = sprintf( ...
        '{"signals":[{"name":%s,"metadata":%s,"files":[%s],"priority":%s}],"overwrite":%s}', ...
        jsonencode(name), DB.encode_json_object(metadata), strjoin(files_json_parts, ','), ...
        jsonencode(priority), jsonencode(overwrite));

      endpoint = sprintf('/stream/%d/dataset/%d/signal/uploads', stream_id, dataset_id);
      presign_resp = obj.post_or_error(endpoint, body, 'Presign signal upload failed for "%s": %s', name);
      DB.must(iscell(presign_resp) || isstruct(presign_resp), 'Unexpected presign response for "%s"', name);
      presigned = DB.find_by_name(presign_resp, name, false);
      DB.must(~isempty(presigned), 'Presign response missing signal "%s"', name);
    end

    function uploaded_files = upload_signal_parts(~, temp_dir, staging_paths, row_counts, dataset_id, presigned, name)
      signal_id = double(presigned.signal_id);
      file_list = DB.as_cell(presigned.files);
      files_by_index = containers.Map('KeyType', 'double', 'ValueType', 'any');
      for i = 1:numel(file_list)
        f = file_list{i};
        files_by_index(double(f.index)) = f;
      end

      uploaded_files = cell(1, numel(row_counts));
      try
        for i = 1:numel(row_counts)
          idx = i - 1;
          DB.must(isKey(files_by_index, idx), 'Presign response missing file index %d for "%s"', idx, name);
          remote = files_by_index(idx);
          upload_path = fullfile(temp_dir, sprintf('upload_%d.parquet', idx));
          meta = DB.prepare_upload_file(staging_paths{i}, upload_path, dataset_id, signal_id, row_counts(i));
          DB.send_file(upload_path, DB.as_char(remote.url), 'PUT');
          uploaded_files{i} = struct( ...
            'path', DB.as_char(remote.path), 'size', double(meta.size), 'footer', double(meta.footer));
        end
      catch ME
        error( ...
          ['Signal upload failed after presign for "%s" (signal_id=%d). ', ...
           'Complete was not called; a FROZEN_TO_COLD placeholder may remain server-side. ', ...
           'Cause: %s'], name, signal_id, ME.message);
      end
    end

    function complete_signal_upload(obj, stream_id, dataset_id, signal_id, name, priority, sum_value, frequency, uploaded_files)
      file_complete_parts = cell(1, numel(uploaded_files));
      for i = 1:numel(uploaded_files)
        f = uploaded_files{i};
        file_complete_parts{i} = sprintf('{"path":%s,"size":%d,"footer":%d}', ...
          jsonencode(f.path), f.size, f.footer);
      end
      body = sprintf( ...
        '{"signals":[{"id":%d,"priority":%s,"stats":{"sum":%s,"frequency":%s},"files":[%s]}]}', ...
        signal_id, jsonencode(priority), ...
        DB.encode_json_nullable_number(sum_value), DB.encode_json_nullable_number(frequency), ...
        strjoin(file_complete_parts, ','));

      endpoint = sprintf('/stream/%d/dataset/%d/signal/uploads/complete', stream_id, dataset_id);
      complete_resp = obj.post_or_error(endpoint, body, ...
        'Complete signal upload failed for "%s" (id=%d): %s', name, signal_id);

      statuses = DB.as_cell(complete_resp);
      DB.must(~isempty(statuses), 'Complete signal upload returned empty status list for id=%d', signal_id);
      ok = cellfun(@(st) isfield(st, 'status') && strcmp(DB.as_char(st.status), 'OK'), statuses);
      if ~all(ok)
        obj.format_upload_statuses(statuses);
      end
    end

    function signal = fetch_signal(obj, stream_id, dataset_id, signal_id, name)
      endpoint = sprintf('/stream/%d/dataset/%d/signal/%d', stream_id, dataset_id, signal_id);
      try
        signal = obj.make_request('GET', endpoint);
      catch ME
        error('Failed to fetch signal "%s" after upload (id=%d): %s', name, signal_id, ME.message);
      end
    end

    function [ingestion_id, dataset_id] = init_ingestion(obj, stream_id, dataset_name, file_size, metadata, overwrite)
      body = sprintf( ...
        '{"stream_id":%d,"dataset_name":%s,"file_size":%d,"metadata":%s,"overwrite":%s}', ...
        stream_id, jsonencode(dataset_name), file_size, ...
        DB.encode_json_object(metadata), jsonencode(overwrite));
      init_resp = obj.post_or_error('/ingestion', body, 'Initialize ingestion failed for "%s": %s', dataset_name);
      DB.must(isfield(init_resp, 'ingestion_id') && isfield(init_resp, 'dataset_id'), ...
        'Initialize ingestion response missing dataset_id/ingestion_id for "%s"', dataset_name);
      ingestion_id = double(init_resp.ingestion_id);
      dataset_id = double(init_resp.dataset_id);
    end

    function upload_ingestion_file(obj, file_path, ingestion_id)
      try
        DB.send_file(file_path, [obj.api_url sprintf('/ingestion/%d/upload/server', ingestion_id)], ...
          'POST', Bearer=obj.api_key, Multipart=true);
        obj.post_json(sprintf('/ingestion/%d/upload/complete', ingestion_id), '{}');
      catch ME
        try
          obj.post_json(sprintf('/ingestion/%d/abort', ingestion_id), ...
            sprintf('{"reason":%s}', jsonencode(ME.message)));
        catch ME2
          warning('Failed to abort ingestion %d: %s', ingestion_id, ME2.message);
        end
        rethrow(ME);
      end
    end
  end

  methods
    function obj = DB(api_url, api_key)
      obj.api_url = api_url;
      obj.api_key = api_key;
    end

    function status = health(obj)
      status = obj.make_request('GET', '/health');
    end

    function streams = get_streams(obj)
      % jsondecode returns [] for an empty list, a struct array when all
      % stream objects have identical fields, and a cell array when they
      % don't. Normalize to a cell array so consumers only handle one shape.
      streams = DB.as_cell(obj.make_request('GET', '/streams').streams);
    end

    function stream = create_stream(obj, name, opts)
      %CREATE_STREAM Create a new datastream.
      %
      %   stream = mdb.create_stream(name)
      %   stream = mdb.create_stream(name, Type="realtime", Description="...")
      %
      %   Type is "files" (default) or "realtime". Optional Description,
      %   Datapool, Plugin, PluginArgs, LayerShifts, SignalReduction,
      %   InsightWorkspace, InsightProject mirror the Python/Rust SDKs.
      arguments
        obj
        name
        opts.Type (1,1) string {mustBeMember(opts.Type, ["files", "realtime"])} = "files"
        opts.Description = []
        opts.Datapool = []
        opts.Plugin = []
        opts.PluginArgs = []
        opts.LayerShifts = []
        opts.SignalReduction = []
        opts.InsightWorkspace = []
        opts.InsightProject = []
      end

      name = DB.require_name(name, 'Stream name');
      body = struct('name', name, 'type', char(opts.Type));
      optional_fields = { ...
        'Description', 'description'; 'Datapool', 'datapool'; ...
        'Plugin', 'plugin'; 'PluginArgs', 'plugin_args'; ...
        'LayerShifts', 'layer_shifts'; 'SignalReduction', 'signal_reduction'; ...
        'InsightWorkspace', 'insight_workspace'; 'InsightProject', 'insight_project' ...
      };
      for i = 1:size(optional_fields, 1)
        value = opts.(optional_fields{i, 1});
        if ~isempty(value)
          body.(optional_fields{i, 2}) = value;
        end
      end

      resp = obj.post_or_error('/stream', jsonencode(body), 'Create stream failed for "%s": %s', name);
      DB.must(isfield(resp, 'id'), 'Create stream response missing id for "%s"', name);
      obj.streams = obj.get_streams();
      stream = DB.find_by_id(obj.streams, double(resp.id));
      DB.must(~isempty(stream), ...
        'Created stream %d ("%s") not found after refreshing stream list', double(resp.id), name);
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

      dataset_name = DB.require_name(dataset_name, 'Dataset name');
      stream_id = obj.find_stream_id(DB.as_char(stream_name));
      body = sprintf('{"dataset_name":%s,"metadata":%s}', ...
        jsonencode(dataset_name), DB.encode_json_object(opts.Metadata));
      endpoint = sprintf('/stream/%d/dataset/add', stream_id);
      resp = obj.post_or_error(endpoint, body, 'Add dataset failed for "%s": %s', dataset_name);
      DB.must(isfield(resp, 'dataset_id'), 'Add dataset response missing dataset_id for "%s"', dataset_name);
      dataset_id = double(resp.dataset_id);
      dataset = obj.fetch_dataset(dataset_id, ...
        'Failed to fetch dataset "%s" after creation (id=%d): %s', dataset_name, dataset_id);
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

      stream_id = obj.find_stream_id(DB.as_char(stream_name));
      endpoint = sprintf('/stream/%d/dataset/%d/metadata', stream_id, dataset_id);
      obj.post_or_error(endpoint, DB.encode_json_object(metadata), ...
        'Update metadata failed for dataset %d: %s', dataset_id);
      dataset = obj.fetch_dataset(dataset_id, ...
        'Failed to fetch dataset %d after metadata update: %s', dataset_id);
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

      name = DB.require_name(name, 'Signal name');
      stream_name = DB.as_char(stream_name);
      priority = char(opts.Priority);
      stream_id = obj.find_stream_id(stream_name);
      dataset = obj.find_dataset(stream_name, dataset_id);

      T = DB.normalize_signal_table(data, name);
      obj.assert_time_overlap(name, T.time, dataset);
      row_counts = DB.plan_row_counts(height(T));

      temp_dir = tempname;
      mkdir(temp_dir);
      cleaner = onCleanup(@() DB.delete_temp_dir(temp_dir)); %#ok<NASGU>

      staging_paths = obj.write_staging_files(T, row_counts, temp_dir);
      presigned = obj.presign_signal_upload( ...
        stream_id, dataset_id, name, row_counts, opts.Metadata, priority, opts.Overwrite);
      signal_id = double(presigned.signal_id);
      uploaded_files = obj.upload_signal_parts( ...
        temp_dir, staging_paths, row_counts, dataset_id, presigned, name);
      obj.complete_signal_upload(stream_id, dataset_id, signal_id, name, priority, ...
        DB.value_sum(T.value), DB.estimate_frequency(T.time), uploaded_files);
      obj.clear_signal_cache(dataset_id, signal_id);
      signal = obj.fetch_signal(stream_id, dataset_id, signal_id, name);
    end

    function dataset = push_file(obj, stream_name, file_path, opts)
      %PUSH_FILE Push a local file to a file stream; ingested as a new dataset.
      %
      %   dataset = mdb.push_file(stream_name, file_path)
      %   dataset = mdb.push_file(..., Metadata=struct(), FileName=name, Overwrite=false)
      %
      %   Uploads via the API server. FileName defaults to the local file's basename and
      %   becomes the dataset name.
      arguments
        obj
        stream_name
        file_path
        opts.Metadata = struct()
        opts.FileName = ""
        opts.Overwrite (1,1) logical = false
      end

      file_path = DB.as_char(file_path);
      DB.must(isfile(file_path), 'File not found: %s', file_path);
      info = dir(file_path);
      if strlength(strtrim(string(opts.FileName))) > 0
        dataset_name = DB.as_char(opts.FileName);
      else
        [~, name, ext] = fileparts(file_path);
        dataset_name = [name ext];
      end

      stream_id = obj.find_stream_id(DB.as_char(stream_name));
      [ingestion_id, dataset_id] = obj.init_ingestion( ...
        stream_id, dataset_name, info(1).bytes, opts.Metadata, opts.Overwrite);
      obj.upload_ingestion_file(file_path, ingestion_id);
      dataset = obj.fetch_dataset(dataset_id, ...
        'Failed to fetch dataset "%s" after push_file (id=%d): %s', dataset_name, dataset_id);
    end

    function dataset = wait_for_import(obj, stream_name, dataset_id, opts)
      %WAIT_FOR_IMPORT Poll a dataset until its import finishes (or times out).
      %
      %   dataset = mdb.wait_for_import(stream_name, dataset_id)
      %   dataset = mdb.wait_for_import(stream_name, dataset_id, Timeout=60)
      %
      %   Returns once import_status leaves the busy set (e.g. FINISHED). If
      %   still busy after Timeout seconds, a warning is issued and the
      %   current dataset is returned.
      arguments
        obj
        stream_name %#ok<INUSA> kept for symmetry with the other flat methods
        dataset_id (1,1) double
        opts.Timeout (1,1) double = 60
      end

      busy_statuses = {'UPLOADING', 'WAITING', 'IMPORTING', 'POSTPROCESSING', 'COOLING'};
      start_time = tic;
      while true
        dataset = obj.fetch_dataset(dataset_id, ...
          'Failed to fetch dataset %d while waiting for import: %s', dataset_id);
        status = DB.as_char(dataset.import_status);
        if ~ismember(status, busy_statuses)
          return;
        end
        if toc(start_time) >= opts.Timeout
          warning('Import for dataset %d did not finish after %g seconds (status: %s)', ...
            dataset_id, opts.Timeout, status);
          return;
        end
        pause(0.5);
      end
    end

    function T = get_data(obj, dataset_path, signal_name, is_text)
      arguments
        obj
        dataset_path
        signal_name
        is_text logical = false
      end
      signal_name = DB.as_char(signal_name);
      dataset_id = obj.get_dataset(struct('path', dataset_path)).id;
      signal_id = obj.find_signal_id(dataset_id, signal_name);
      cache = obj.signal_cache_path(dataset_id, signal_id);

      if ~isfolder(cache) || isempty(dir(fullfile(cache, '*.parquet')))
        if ~isfolder(cache)
          mkdir(cache);
        end
        endpoint = sprintf('/datapool/%s/dataset/%d/signal/%d/data', obj.datapool, dataset_id, signal_id);
        paths = obj.make_request('GET', endpoint);
        for i = 1:length(paths)
          url = paths{i};
          [~, name, ext] = fileparts(extractBefore(url, '?'));
          websave(fullfile(cache, [name ext]), url);
        end
      end

      try
        T = readall(parquetDatastore(cache));
      catch
        DB.transcode_cache(cache);
        T = readall(parquetDatastore(cache));
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
      cache = obj.workspace_cache();
      if isfolder(cache)
        rmdir(cache, 's');
      end
    end
  end

end
