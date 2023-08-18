from typing import Literal
import pandas as pd
from pandas import HDFStore
from nilmtk import DataSet

from utils.constants import DatasetType, DURATION_MAX_GAP_PAIRS, FULL_KEY_PATTERN
from utils.parallelizer import parallelize
from utils.preprocessing import clear_nulls, interpolate_missed_data, \
    reformat_to_accumulated, get_stable_periods
from utils.timing import time_measure


def convert_to_nilmtk_format(ds: DatasetType) -> None:
    with time_measure(f'converting {ds.name} dataset to nilmtk format'):
        ds.nilmtk_converter(ds.archive_path(), ds.nilmtk_path())


def clean_and_store_data(ds: DatasetType) -> None:
    with time_measure(f'cleaning and storing {ds.name} dataset from nilmtk format'):
        nilmtk_data = DataSet(ds.nilmtk_path())
        keys = nilmtk_data.buildings.keys()
        data_file = HDFStore(ds.cleaned_path(), mode='w', complevel=5)

        for key in keys:
            elec = nilmtk_data.buildings[key].elec
            data = elec.mains().power_series_all_data()
            data = clear_nulls(data)
            data_file['building_' + str(key)] = data

        nilmtk_data.store.close()
        data_file.close()


def read_cleaned_data(ds: DatasetType) -> dict[str, pd.Series]:
    with time_measure(f'reading cleaned {ds.name} dataset'):
        data = dict()
        data_file = HDFStore(ds.cleaned_path(), mode='r')
        for key in data_file.keys():
            data[key] = data_file[key]
        data_file.close()
        return data


def build_full_key(key: str, duration: int, max_gap: int, index: int | Literal['']) -> str:
    return f'{key}/dur_{duration}/gap_{max_gap}/n_{index}'


def parse_full_key(full_key: str) -> (str, int, int, int):
    m = FULL_KEY_PATTERN.match(full_key)
    key, duration, max_gap, index = m.group(1, 2, 3, 4)
    return key, int(duration), int(max_gap), int(index)


def store_processed_stable_periods(data: pd.Series, ds: DatasetType, key: str, periods: list[slice], duration: int, max_gap: int) -> None:
    with time_measure(f'storing stable periods of {ds.name}{key} with duration = {duration}s and max_gap = {max_gap}s', is_active=False):
        data_file = HDFStore(ds.periods_path(), mode='a', complevel=9)
        data_file_power = HDFStore(ds.periods_path(power_mode=True), mode='a', complevel=9)
        for i, p in enumerate(periods):
            stable_period = data.iloc[p]
            interpolated = interpolate_missed_data(stable_period, duration=duration)
            accumulated = reformat_to_accumulated(interpolated)
            data_file[build_full_key(key, duration, max_gap, i)] = accumulated
            data_file_power[build_full_key(key, duration, max_gap, i)] = interpolated
        data_file.close()
        data_file_power.close()


def process_stable_periods(ds: DatasetType, duration: int, max_gap: int) -> None:
    with time_measure(f'processing stable periods for {ds.name} dataset'):
        data_file = HDFStore(ds.cleaned_path(), mode='r')
        for key in data_file.keys():
            data = data_file[key]
            periods = get_stable_periods(data, duration, max_gap)
            store_processed_stable_periods(data, ds, key, periods, duration, max_gap)
        data_file.close()


def get_full_keys_of_stable_periods(ds: DatasetType, key: str | None = None, power_mode: bool = False) -> list[str]:
    data_file = HDFStore(ds.periods_path(power_mode), mode='r')
    if key is None:
        keys = data_file.keys()
    else:
        keys = [k for k in data_file.keys() if key in k]
    data_file.close()
    return keys


def read_stable_periods(ds: DatasetType, full_keys: list[str], power_mode: bool = False) -> list[pd.Series]:
    with time_measure(f'reading {len(full_keys)} stable periods of {ds.name}'):
        data = []
        data_file = HDFStore(ds.periods_path(power_mode), mode='r')
        for key in full_keys:
            data.append(data_file[key])
        data_file.close()
        return data


def read_stable_period(ds: DatasetType, full_key: str, power_mode: bool = False) -> pd.Series:
    with time_measure(f'reading stable period of {ds.name}', is_active=False):
        data_file = HDFStore(ds.periods_path(power_mode), mode='r')
        data = data_file[full_key]
        data_file.close()
        return data


def prepare_data_for_experiment(ds: DatasetType):
    for (dur, gap) in DURATION_MAX_GAP_PAIRS:
        process_stable_periods(ds, dur, gap)
        print(ds, dur, gap)

