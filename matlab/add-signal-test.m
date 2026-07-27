%% add-signal-test.m
% Generate a short wind-turbine SCADA burst and upload it with add_signal.
% Inspired by python/ingest-ibiza.py (synthetic/derived signals + overwrite).
%
% Prerequisites:
%   - matlab/config.json with a valid api_key
%   - an existing imported dataset in STREAM_NAME (MATLAB cannot create
%     empty realtime datasets yet; pick one whose time range you may write into)
%   - parquet-transcode v0.2.0+ available (auto-downloaded on first use)
%
% Run from the repo root:
%   addpath(genpath(fullfile(pwd, 'matlab')))
%   run(fullfile('matlab', 'add-signal-test.m'))

%% Config — edit these
STREAM_NAME = 'Charles river measurements';
DATASET_ID = [];          % [] = first dataset in the stream
N_SAMPLES = 600;          % 10 min at 1 Hz
SAMPLE_PERIOD_NS = int64(1e9);
TURBINE_ID = 'WTG-01';
OVERWRITE = true;

%% Connect
mdb = DB.from_config();
fprintf('Connected to %s (workspace=%s, datapool=%s)\n', ...
  mdb.api_url, mdb.workspace, mdb.datapool);

datasets = mdb.get_datasets(STREAM_NAME);
if isempty(datasets)
  error('No datasets in stream "%s"', STREAM_NAME);
end
if iscell(datasets)
  ds_list = datasets;
else
  ds_list = num2cell(datasets);
end

if isempty(DATASET_ID)
  dataset = ds_list{1};
else
  dataset = [];
  for i = 1:numel(ds_list)
    if double(ds_list{i}.id) == double(DATASET_ID)
      dataset = ds_list{i};
      break;
    end
  end
  if isempty(dataset)
    error('Dataset id %d not found in stream "%s"', DATASET_ID, STREAM_NAME);
  end
end
dataset_id = double(dataset.id);
fprintf('Target dataset path=%s id=%d\n', char(string(dataset.path)), dataset_id);

%% Time base overlapping the dataset range when known
t0 = int64(1.577e18); % ~2020-01-01 UTC fallback
if isfield(dataset, 'timestamp_start') && ~isempty(dataset.timestamp_start)
  t0 = int64(dataset.timestamp_start);
end
if isfield(dataset, 'timestamp_stop') && ~isempty(dataset.timestamp_stop)
  t_stop = int64(dataset.timestamp_stop);
  span = t_stop - t0;
  need = int64(N_SAMPLES - 1) * SAMPLE_PERIOD_NS;
  if span > need
    % Centre the burst in the dataset window.
    t0 = t0 + idivide(span - need, int64(2), 'floor');
  end
end
time = t0 + int64((0:N_SAMPLES-1)') * SAMPLE_PERIOD_NS;

%% Synthetic wind-turbine SCADA (1 Hz)
rng(42);
k = (0:N_SAMPLES-1)' / double(N_SAMPLES);
wind_speed = 8 + 3.5 * sin(2 * pi * 2 * k) + 0.6 * randn(N_SAMPLES, 1);
wind_speed = max(wind_speed, 0.5);
rotor_rpm = min(max(wind_speed * 1.35 + 0.4 * randn(N_SAMPLES, 1), 0), 18);
% Simple power curve proxy (cut-in ~3 m/s, rated ~12 m/s @ 2 MW).
power_kw = zeros(N_SAMPLES, 1);
for i = 1:N_SAMPLES
  v = wind_speed(i);
  if v < 3
    power_kw(i) = 0;
  elseif v < 12
    power_kw(i) = 2000 * ((v - 3) / 9) ^ 3;
  else
    power_kw(i) = 2000;
  end
end
power_kw = power_kw + 15 * randn(N_SAMPLES, 1);
power_kw = max(power_kw, 0);
nacelle_yaw = 210 + 8 * sin(2 * pi * k) + 0.5 * randn(N_SAMPLES, 1);
generator_temp = 55 + 0.008 * power_kw + 0.8 * randn(N_SAMPLES, 1);
pitch_deg = max(0, (wind_speed - 12) * 2.5) + 0.2 * abs(randn(N_SAMPLES, 1));
% Text status: producing / idle / pitched
status = strings(N_SAMPLES, 1);
for i = 1:N_SAMPLES
  if power_kw(i) < 50
    status(i) = "idle";
  elseif pitch_deg(i) > 5
    status(i) = "pitched";
  else
    status(i) = "producing";
  end
end

uploads = { ...
  struct( ...
    'name', sprintf('turbine.%s.wind_speed', TURBINE_ID), ...
    'unit', 'm/s', ...
    'description', 'Hub-height wind speed', ...
    'data', table(time, wind_speed, 'VariableNames', {'time', 'value'})); ...
  struct( ...
    'name', sprintf('turbine.%s.rotor_rpm', TURBINE_ID), ...
    'unit', 'rpm', ...
    'description', 'Rotor speed', ...
    'data', table(time, rotor_rpm, 'VariableNames', {'time', 'value'})); ...
  struct( ...
    'name', sprintf('turbine.%s.power_kw', TURBINE_ID), ...
    'unit', 'kW', ...
    'description', 'Active power (synthetic power-curve proxy)', ...
    'data', table(time, power_kw, 'VariableNames', {'time', 'value'})); ...
  struct( ...
    'name', sprintf('turbine.%s.nacelle_yaw', TURBINE_ID), ...
    'unit', 'deg', ...
    'description', 'Nacelle yaw angle', ...
    'data', table(time, nacelle_yaw, 'VariableNames', {'time', 'value'})); ...
  struct( ...
    'name', sprintf('turbine.%s.generator_temp', TURBINE_ID), ...
    'unit', 'C', ...
    'description', 'Generator bearing temperature', ...
    'data', table(time, generator_temp, 'VariableNames', {'time', 'value'})); ...
  struct( ...
    'name', sprintf('turbine.%s.pitch_deg', TURBINE_ID), ...
    'unit', 'deg', ...
    'description', 'Blade pitch angle', ...
    'data', table(time, pitch_deg, 'VariableNames', {'time', 'value'})); ...
  struct( ...
    'name', sprintf('turbine.%s.status', TURBINE_ID), ...
    'unit', '-', ...
    'description', 'Discrete operating state', ...
    'data', table(time, status, 'VariableNames', {'time', 'value_text'})); ...
};

fprintf('Uploading %d wind-turbine signals (%d samples @ 1 Hz) → dataset %d...\n', ...
  numel(uploads), N_SAMPLES, dataset_id);

for i = 1:numel(uploads)
  u = uploads{i};
  meta = struct( ...
    'unit', u.unit, ...
    'description', u.description, ...
    'turbine_id', TURBINE_ID, ...
    'source', 'matlab/add-signal-test.m');
  fprintf('  [%d/%d] %s ...\n', i, numel(uploads), u.name);
  signal = mdb.add_signal( ...
    STREAM_NAME, dataset_id, u.name, u.data, ...
    Metadata=meta, Overwrite=OVERWRITE);
  sid = [];
  if isfield(signal, 'id')
    sid = signal.id;
  end
  fprintf('    id=%s\n', string(sid));
end

fprintf( ...
  'Done. Stream=%s dataset=%s id=%d signals=%d (Iceberg commit may still be running)\n', ...
  STREAM_NAME, char(string(dataset.path)), dataset_id, numel(uploads));
