import numpy as np
from nilmtk import STATS_CACHE

from utils.constants import DatasetType, DAY_IN_SEC
from utils.data_reading import process_data, read_processed_data, \
    convert_to_nilmtk_format, store_processed_stable_periods, \
    get_full_keys_of_stable_periods, read_stable_periods
from utils.preprocessing import generate_sync_signals, get_stable_periods, \
    interpolate_missed_data, generate_async_signals, reformat_to_accumulated
from utils.timing import time_measure

STATS_CACHE.store.close()


DURATION = DAY_IN_SEC*30
MAX_GAP = 300

# convert_to_nilmtk_format(DatasetType.REDD)
# process_data(DatasetType.IDEAL)
ideal = read_processed_data(DatasetType.IDEAL)

key0, data0 = next(iter(ideal.items()))
periods = get_stable_periods(data0, duration=DURATION, max_gap=MAX_GAP)
# stable_period = data0.iloc[periods[0]]

store_processed_stable_periods(data0, DatasetType.IDEAL, key0, periods, duration=DURATION, max_gap=MAX_GAP)

# interpolated = interpolate_missed_data(stable_period, duration=DURATION)

# accumulated = reformat_to_accumulated(interpolated)

full_keys = get_full_keys_of_stable_periods(DatasetType.IDEAL, key0)
accumulated = read_stable_periods(DatasetType.IDEAL, [full_keys[0]])[0]

sync_signals = generate_sync_signals(accumulated, 100)
# print('len(sync_signals)', len(sync_signals))
# print(sync_signals.head())
# print(sync_signals.tail())

async_signals = generate_async_signals(accumulated, 10000.5)
# print('len(async_signals)', len(async_signals))
# print(async_signals.head())
# print(async_signals.tail())
