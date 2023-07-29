import numpy as np
from nilmtk import STATS_CACHE

from utils.constants import DatasetType, DAY_IN_SEC
from utils.data_reading import clean_and_store_data, read_cleaned_data, \
    convert_to_nilmtk_format, store_processed_stable_periods, \
    get_full_keys_of_stable_periods, read_stable_periods
from utils.informative_index import accumulated_distance
from utils.preprocessing import generate_sync_signals, get_stable_periods, \
    interpolate_missed_data, generate_async_signals, reformat_to_accumulated
from utils.timing import time_measure
from utils.visualization import plot_sync_async_comparison

STATS_CACHE.store.close()


DS = DatasetType.IDEAL
DURATION = DAY_IN_SEC*30
MAX_GAP = 300

# convert_to_nilmtk_format(DS)
# clean_and_store_data(DS)
# ideal = read_cleaned_data(DS)
# process_stable_periods(DS, DURATION, MAX_GAP)

if __name__ == '__main__':
    full_keys = get_full_keys_of_stable_periods(DS)
    accumulated = read_stable_periods(DS, [full_keys[0]])[0]

    sync_signals = generate_sync_signals(accumulated, 100)
    print('len(sync_signals)', len(sync_signals))

    async_signals = generate_async_signals(accumulated, 60000)
    print('len(async_signals)', len(async_signals))

    # plot_sync_async_comparison(accumulated, sync_signals, async_signals)
    accumulated_distance(accumulated, async_signals, 4)
