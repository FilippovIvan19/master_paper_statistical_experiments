import argparse

import numpy as np
from pandas import HDFStore

from nilmtk import STATS_CACHE

from utils.constants import DatasetType, DAY_IN_SEC, DURATION_MAX_GAP_PAIRS, TIME_DELTAS
# from utils.data_reading import process_stable_periods
# from utils.data_reading import clean_and_store_data, read_cleaned_data, \
#     convert_to_nilmtk_format, store_processed_stable_periods, \
#     get_full_keys_of_stable_periods, read_stable_periods, process_stable_periods, \
#     build_full_key, parse_full_key
from utils.experiment import run_experiment_accum, run_experiment_power
# from utils.informative_index import accumulated_distance, power_distance
# from utils.preprocessing import generate_sync_signals, get_stable_periods, \
#     interpolate_missed_data, generate_async_signals, reformat_to_accumulated, \
#     generate_sync_signals_by_point_count, generate_async_signals_by_point_count
from utils.timing import time_measure
# from utils.visualization import plot_sync_async_comparison

STATS_CACHE.store.close()


DS = DatasetType.IDEAL
# DURATION = DAY_IN_SEC*30
# MAX_GAP = 300

# convert_to_nilmtk_format(DS)
# clean_and_store_data(DS)
# ideal = read_cleaned_data(DS)
# process_stable_periods(DS, DURATION, MAX_GAP)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('experiment', choices=['accum', 'power'])
    parser.add_argument('dataset', choices=[elem.path_str for elem in DatasetType])
    parser.add_argument('time_delta_idx', type=int, choices=range(len(TIME_DELTAS)))
    parser.add_argument('dur_gap_start', type=int, choices=range(len(DURATION_MAX_GAP_PAIRS)))
    parser.add_argument('dur_gap_end', type=int, choices=range(1, len(DURATION_MAX_GAP_PAIRS)+1))
    parser.add_argument('processes', type=int, nargs='?', default=1)
    args = parser.parse_args()

    if args.experiment == 'accum':
        experiment_func = run_experiment_accum
        experiment_name = 'run_experiment_accum'
    else:
        experiment_func = run_experiment_power
        experiment_name = 'run_experiment_power'

    with time_measure(experiment_name):
        experiment_func(
            DatasetType.get_by_name(args.dataset),
            TIME_DELTAS[args.time_delta_idx],
            f'{args.experiment}_{args.dataset}_td{TIME_DELTAS[args.time_delta_idx]}_dur_gap{args.dur_gap_start}-{args.dur_gap_end}.xlsx',
            slice(args.dur_gap_start, args.dur_gap_end),
            args.processes
        )



    # print(DatasetType.REDD)
    # print(DatasetType.get_by_name('redd'))
    # print(DatasetType.get_by_name('ideal'))
    # print(DatasetType.get_by_name('hjkdfshjshjk'))


    # dur, gap = DURATION_MAX_GAP_PAIRS[-1]
    # print(dur//DAY_IN_SEC, gap)
    # process_stable_periods(DS, dur, gap)


    # print(len(get_full_keys_of_stable_periods(DatasetType.REDD)))
    # print(len(get_full_keys_of_stable_periods(DatasetType.IDEAL)))

    # for dur, gap in DURATION_MAX_GAP_PAIRS[22:25]:
    #     print(dur//DAY_IN_SEC, gap)
    #     process_stable_periods(DS, dur, gap)
    # #     # print(len(get_full_keys_of_stable_periods(DS)))


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
