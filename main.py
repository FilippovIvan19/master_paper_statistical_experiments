import numpy as np
from nilmtk import STATS_CACHE

from utils.constants import DatasetType, DAY_IN_SEC
from utils.data_reading import process_data, read_processed_data, convert_to_nilmtk_format
from utils.preprocessing import generate_sync_signals, get_stable_periods, \
    interpolate_missed_data, generate_async_signals, reformat_to_accumulated
from utils.timing import time_measure

STATS_CACHE.store.close()


# convert_to_nilmtk_format(DatasetType.REDD)
# process_data(DatasetType.IDEAL)
ideal = read_processed_data(DatasetType.IDEAL)

data0 = next(iter(ideal.values()))
# periods = get_stable_periods(data0, duration=DAY_IN_SEC, max_gap=5)
periods = get_stable_periods(data0, duration=DAY_IN_SEC*30, max_gap=300)
stable_period = data0.iloc[periods[0]]

# interpolated = interpolate_missed_data(stable_period, duration=DAY_IN_SEC)
interpolated = interpolate_missed_data(stable_period, duration=DAY_IN_SEC*30)

accumulated = reformat_to_accumulated(interpolated)

sync_signals = generate_sync_signals(accumulated, 100)
# print('len(sync_signals)', len(sync_signals))
# print(sync_signals.head())
# print(sync_signals.tail())

async_signals = generate_async_signals(accumulated, 10000.5)
# print('len(async_signals)', len(async_signals))
# print(async_signals.head())
# print(async_signals.tail())
