%% Load configuration from config.json
mdb = DB.from_config();

%% Example 1: Calculating max(turbidity) of 2020

T = mdb.get_data('charles-river-2020_clean.csv', 'turbidity');
max_turbidity = max(T.turbidity);
disp(max_turbidity)

%% Example 2: Boxplot of every pH per year

datasets = mdb.get_datasets('Charles river measurements');
T = table();

for i = 1:length(datasets)
  dataset = datasets(i);
  fprintf('Fetching %s\n', dataset.path)
  current_T = mdb.get_data(dataset.path, 'pH');
  current_T.year = repmat(dataset.metadata.Year, height(current_T), 1);
  T = [T; current_T];
end

figure;
boxplot(T.pH, T.year);
xlabel('Year')
ylabel('pH');

%% Example 3: 3D scatter (2020)

file_name = 'charles-river-2020_clean.csv';

TT_turb = toTT(mdb.get_data(file_name, 'turbidity'), 'turbidity');
TT_temp = toTT(mdb.get_data(file_name, 'temp'), 'temp');
TT_chl  = toTT(mdb.get_data(file_name, 'chlorophyll'), 'chlorophyll');
TT_ph   = toTT(mdb.get_data(file_name, 'pH'), 'pH');

% align timestamps (nearest where timestamps differ)
TT_all = synchronize(TT_turb, TT_temp, TT_chl, TT_ph, 'union', 'nearest');
figure;
scatter3( ...
    TT_all.turbidity, ...     % X
    TT_all.chlorophyll, ...   % Y
    TT_all.pH, ...            % Z
    36, ...                   % marker size
    TT_all.temp, ...          % color
    'filled' ...
);
xlabel('Turbidity');
ylabel('Chlorophyll');
zlabel('pH');
cb = colorbar;
ylabel(cb,'Temperature');

%% Example 4: ingest Simulink signals (commented — creates a dataset)
%
% Assumes signals is a Simulink.SimulationData.Dataset of scalar signals whose
% Values.Time is measured in seconds from the start of the run. Uncomment and
% set stream_name before running.
%
% stream_name = 'Simulation';
% run_start = datetime('now', 'TimeZone', 'UTC');
% dataset = mdb.add_dataset( ...
%   stream_name, 'simulation-run', ...
%   Metadata=struct('source', 'Simulink'));
%
% for i = 1:numElements(signals)
%   logged = signals{i};
%   values = logged.Values;
%   data = timetable( ...
%     run_start + seconds(values.Time(:)), ...
%     values.Data(:), ...
%     'VariableNames', {'value'});
%   mdb.add_signal(stream_name, dataset.id, logged.Name, data);
% end
%
% dataset = mdb.update_metadata( ...
%   stream_name, dataset.id, ...
%   struct( ...
%     'model_name', bdroot, ...
%     'solver', get_param(bdroot, 'Solver'), ...
%     'scenario', 'baseline');
% disp(dataset);

%% Example 5: push a local file to a stream 
%
% stream_name = 'CSV Stream';
% dataset = mdb.push_file( ...
%   stream_name, 'race.csv', ...
%   Metadata=struct('car_id', 1, 'track', 'track_1', 'weather', 'sunny'));
% dataset = mdb.wait_for_import(stream_name, dataset.id);
% T = mdb.get_data(dataset.path, 'speed');
% disp(dataset);

%% Example 6: create a stream if it doesn't exist yet, then push a .mat file to it
%
% stream_name = 'Matlab SDK test';
% streams = mdb.get_streams();
% stream_names = cellfun(@(s) s.name, streams, 'UniformOutput', false);
% if ~any(strcmpi(stream_names, stream_name))
%   mdb.create_stream(stream_name, Type='files', Plugin='MATLAB');
% end
% dataset = mdb.push_file(stream_name, 'run.mat');
% dataset = mdb.wait_for_import(stream_name, dataset.id);
% disp(dataset);

function TT = toTT(tbl, signalName)
    t = datetime(tbl.time/1e9, 'ConvertFrom','posixtime', 'TimeZone','UTC');
    TT = table2timetable(table(t, tbl{:,signalName}, 'VariableNames', {'time', signalName}), 'RowTimes','time');
    TT = retime(TT, 'regular', 'nearest', 'TimeStep', minutes(30)); % bucket to nearest 30 minutes
end
