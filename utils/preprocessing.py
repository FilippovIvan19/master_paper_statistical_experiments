import numpy as np
import pandas as pd
from scipy.interpolate import PchipInterpolator  # PCHIP 1-D monotonic cubic interpolation
from scipy.interpolate import CubicSpline

from utils.constants import MONTH_IN_SEC
from utils.timing import time_measure


def clear_nulls(data: pd.Series) -> pd.Series:
    new_data = pd.to_numeric(data, errors='coerce')
    new_data.replace([np.inf, -np.inf], np.nan, inplace=True)
    new_data = new_data.dropna()
    return new_data[new_data >= 0]


def get_stable_periods(data: pd.Series, duration: int = MONTH_IN_SEC,
                       max_gap: int = 5) -> list[slice]:
    int_dates = data.index.astype(np.int64) // 10 ** 9
    with time_measure(f'getting stable periods'):
        stable_period_start_indexes = [0]
        stable_period_end_indexes = []
        deltas = [0] + [
            int_dates[i] - int_dates[i - 1]
            for i in range(1, len(int_dates))
        ]

        interval_lens = [1] * len(int_dates)
        for i in range(len(int_dates)):
            if interval_lens[i - 1] < duration:
                if deltas[i] <= max_gap:
                    interval_lens[i] = interval_lens[i - 1] + deltas[i]
                else:
                    stable_period_start_indexes.pop()
                    stable_period_start_indexes.append(i)
            else:
                stable_period_start_indexes.append(i)
                stable_period_end_indexes.append(i)  # end border excluded

        if len(stable_period_start_indexes) > len(stable_period_end_indexes):
            stable_period_start_indexes.pop()
        return [slice(a, b) for a, b
                in zip(stable_period_start_indexes, stable_period_end_indexes)]


def interpolate_missed_data(data: pd.Series, duration: int) -> pd.Series:
    with time_measure(f'interpolation'):
        int_dates = data.index.astype(np.int64) // 10 ** 9
        all_dates = np.arange(int_dates[0], int_dates[-1] + 1)
        interpolation_func = CubicSpline(int_dates, data.values)
        formatted_dates = pd.to_datetime(all_dates, unit='s', utc=True).tz_convert(tz=data.index[0].tz)
        new_values = interpolation_func(all_dates)
        new_values[new_values < 0] = 0
        return pd.Series(new_values, index=formatted_dates).iloc[:duration]


def reformat_to_accumulated(data: pd.Series) -> pd.Series:
    with time_measure(f'reformatting to accumulated'):
        zero_date = data.index[0] - pd.Timedelta(seconds=1)
        indexes = pd.Index([zero_date]).append(data.index)
        values = data.values
        values[values <= 0] = 1e-7
        accumulated = np.concatenate(([0], values.cumsum()), axis=0)
        return pd.Series(accumulated, index=indexes)


def generate_sync_signals(data: pd.Series, time_delta: int) -> pd.Series:
    with time_measure(f'generating sync signals'):
        return data[::time_delta]


def generate_async_signals(data: pd.Series, resource_delta: float) -> pd.Series:
    with time_measure(f'generating async signals'):
        signal_count = int(data.values[-1] // resource_delta) + 1
        signals = np.array([resource_delta * i for i in range(signal_count)])

        int_dates = data.index.astype(np.int64) // 10 ** 9
        interpolation_func = PchipInterpolator(data.values, int_dates)
        new_dates = interpolation_func(signals)
        formatted_dates = pd.to_datetime(new_dates, unit='s', utc=True).tz_convert(tz=data.index[0].tz)
        return pd.Series(signals, index=formatted_dates)
