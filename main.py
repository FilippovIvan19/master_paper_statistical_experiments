import argparse

from nilmtk import STATS_CACHE

from utils.constants import DatasetType, DURATION_MAX_GAP_PAIRS, TIME_DELTAS
from utils.experiment import run_experiment_accum, run_experiment_power
from utils.timing import time_measure

STATS_CACHE.store.close()  # to avoid warning about non-closed file


# launch examples:
# python main.py power ideal 7 11 12 4
# python main.py accum redd 0 20 30

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('experiment', choices=['accum', 'power'])
    parser.add_argument('dataset', choices=[elem.path_str for elem in DatasetType])
    parser.add_argument('time_delta_idx', type=int, choices=range(len(TIME_DELTAS)))
    parser.add_argument('dur_gap_start', type=int, choices=range(len(DURATION_MAX_GAP_PAIRS)))
    parser.add_argument('dur_gap_end', type=int, choices=range(1, len(DURATION_MAX_GAP_PAIRS)+1))
    parser.add_argument('processes', type=int, nargs='?', default=1)
    args = parser.parse_args()

    by_resource_delta = True  # use this param to change limitation
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
            args.processes,
            by_resource_delta
        )
