import numpy as np
from pandas import HDFStore

from nilmtk import STATS_CACHE

from utils.constants import DatasetType, DAY_IN_SEC, DURATION_MAX_GAP_PAIRS
from utils.data_reading import clean_and_store_data, read_cleaned_data, \
    convert_to_nilmtk_format, store_processed_stable_periods, \
    get_full_keys_of_stable_periods, read_stable_periods, process_stable_periods
from utils.experiment import run_experiment_accum
from utils.informative_index import accumulated_distance, power_distance
from utils.preprocessing import generate_sync_signals, get_stable_periods, \
    interpolate_missed_data, generate_async_signals, reformat_to_accumulated, \
    generate_sync_signals_by_point_count, generate_async_signals_by_point_count
from utils.timing import time_measure
from utils.visualization import plot_sync_async_comparison

STATS_CACHE.store.close()


DS = DatasetType.REDD
DURATION = DAY_IN_SEC*30
MAX_GAP = 300

# convert_to_nilmtk_format(DS)
# clean_and_store_data(DS)
# ideal = read_cleaned_data(DS)
# process_stable_periods(DS, DURATION, MAX_GAP)

if __name__ == '__main__':
    with time_measure(f'run_experiment_accum'):
        run_experiment_accum(DatasetType.REDD, 60, "output.xlsx", slice(-6, -4), 6)


    # dur, gap = DURATION_MAX_GAP_PAIRS[-1]
    # print(dur//DAY_IN_SEC, gap)
    # process_stable_periods(DS, dur, gap)


    # print(len(get_full_keys_of_stable_periods(DS)))
    # for dur, gap in DURATION_MAX_GAP_PAIRS[0: -4]:
    #     print(dur//DAY_IN_SEC, gap)
    #     process_stable_periods(DS, dur, gap)
    #     print(len(get_full_keys_of_stable_periods(DS)))


    # full_keys = get_full_keys_of_stable_periods(DS)
    # accumulated = read_stable_periods(DS, [full_keys[0]])[0]
    # # power = read_stable_periods(DS, [full_keys[0]], power_mode=True)[0]
    #
    # time_delta = 100
    # sync_signals = generate_sync_signals(accumulated, time_delta)
    # point_count = len(sync_signals)
    # # print('len(sync_signals)', len(sync_signals))
    #
    # async_signals = generate_async_signals_by_point_count(accumulated, point_count)
    # # print('len(async_signals)', len(async_signals))
    #
    # # plot_sync_async_comparison(accumulated, sync_signals, async_signals)
    # accumulated_distance(accumulated, async_signals, 4)
    # # power_distance(power, sync_signals, 4)
