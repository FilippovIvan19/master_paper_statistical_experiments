import pandas as pd

from utils.constants import DatasetType, DURATION_MAX_GAP_PAIRS, EXP_RESULTS_PATH
from utils.data_reading import get_full_keys_of_stable_periods, parse_full_key, \
    build_full_key, read_stable_period
from utils.informative_index import accumulated_distance, power_distance
from utils.parallelizer import parallelize
from utils.preprocessing import generate_sync_signals, \
    generate_async_signals_by_point_count
from utils.timing import time_measure


def compute_accum_dist_info(ds: DatasetType, full_key: str, time_delta: int) -> tuple:
    with time_measure(f'compute_accum_dist_info {ds.name}{full_key}'):
        accumulated = read_stable_period(ds, full_key)

        sync_signals = generate_sync_signals(accumulated, time_delta)
        point_count = len(sync_signals)
        resource_delta = accumulated.values[-1] / point_count
        async_signals = generate_async_signals_by_point_count(accumulated, point_count)

        sync_dist = accumulated_distance(accumulated, sync_signals)
        async_dist = accumulated_distance(accumulated, async_signals)

        key, duration, max_gap, index = parse_full_key(full_key)
        info = (ds.name, key, duration, max_gap, index, time_delta, point_count,
                resource_delta, sync_dist, async_dist)
        return info


def run_experiment_accum(ds: DatasetType, time_delta: int, out_file: str, part: slice = slice(None), processes: int = 1) -> None:
    with parallelize(processes, []) as parallelizer:
        for (dur, gap) in DURATION_MAX_GAP_PAIRS[part]:
            key_base = build_full_key('', dur, gap, '')
            full_keys = get_full_keys_of_stable_periods(ds, key_base)
            for full_key in full_keys:
                parallelizer.apply_async(compute_accum_dist_info, (ds, full_key, time_delta))

    df = pd.DataFrame(data=parallelizer.storage,
                      columns=['ds', 'key', 'duration', 'max_gap', 'index', 'time_delta',
                               'point_count', 'resource_delta', 'sync_dist', 'async_dist'])
    df.to_excel(EXP_RESULTS_PATH + out_file, sheet_name='accumulated_distance')


def compute_power_dist_info(ds: DatasetType, full_key: str, time_delta: int) -> tuple:
    with time_measure(f'compute_power_dist_info {ds.name}{full_key}'):
        accumulated = read_stable_period(ds, full_key)
        power = read_stable_period(ds, full_key, power_mode=True)

        sync_signals = generate_sync_signals(accumulated, time_delta)
        point_count = len(sync_signals)
        resource_delta = accumulated.values[-1] / point_count
        async_signals = generate_async_signals_by_point_count(accumulated, point_count)

        sync_dist = power_distance(power, sync_signals)
        async_dist = power_distance(power, async_signals)

        key, duration, max_gap, index = parse_full_key(full_key)
        info = (ds.name, key, duration, max_gap, index, time_delta, point_count,
                resource_delta, sync_dist, async_dist)
        return info


def run_experiment_power(ds: DatasetType, time_delta: int, out_file: str, part: slice = slice(None), processes: int = 1) -> None:
    with parallelize(processes, []) as parallelizer:
        for (dur, gap) in DURATION_MAX_GAP_PAIRS[part]:
            key_base = build_full_key('', dur, gap, '')
            full_keys = get_full_keys_of_stable_periods(ds, key_base, power_mode=True)
            for full_key in full_keys:
                parallelizer.apply_async(compute_power_dist_info, (ds, full_key, time_delta))

    df = pd.DataFrame(data=parallelizer.storage,
                      columns=['ds', 'key', 'duration', 'max_gap', 'index', 'time_delta',
                               'point_count', 'resource_delta', 'sync_dist', 'async_dist'])
    df.to_excel(EXP_RESULTS_PATH + out_file, sheet_name='power_distance')
