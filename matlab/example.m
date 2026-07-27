%% Load configuration from config.yaml
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

function TT = toTT(tbl, signalName)
    t = datetime(tbl.time/1e9, 'ConvertFrom','posixtime', 'TimeZone','UTC');
    TT = table2timetable(table(t, tbl{:,signalName}, 'VariableNames', {'time', signalName}), 'RowTimes','time');
    TT = retime(TT, 'regular', 'nearest', 'TimeStep', minutes(30)); % bucket to nearest 30 minutes
end

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

%% Example 4: add_signal (commented — mutates a dataset)
%
% Uploads a small numeric signal onto an existing imported dataset.
% Uncomment and set stream_name / dataset_id for a dataset you own.
% For a fuller wind-turbine demo, see add_signal_test.m.
%
% stream_name = 'Charles river measurements';
% datasets = mdb.get_datasets(stream_name);
% dataset_id = datasets(1).id;
% n = 100;
% t0 = int64(datasets(1).timestamp_start);
% if isempty(t0)
%   t0 = int64(1.577e18); % fallback ~2020-01-01 UTC in ns
% end
% data = table( ...
%   t0 + int64((0:n-1)' * 1e9), ...
%   rand(n, 1), ...
%   'VariableNames', {'time', 'value'});
% signal = mdb.add_signal( ...
%   stream_name, dataset_id, 'sdk.matlab.demo', data, ...
%   Metadata=struct('unit', '1'), Overwrite=true);
% disp(signal);
