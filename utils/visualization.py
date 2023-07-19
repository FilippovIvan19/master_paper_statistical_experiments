import matplotlib.pyplot as plt
import pandas as pd

from utils.preprocessing import datetime_index_to_ints


def plot_power_and_consumption(data: pd.Series, period: slice = slice(None)) -> None:
    int_dates = datetime_index_to_ints(data.index)
    accumulated = data.values[period].cumsum()
    fig, axs = plt.subplots(1, 2, figsize=(13, 5))
    axs[0].plot(int_dates[period], data.values[period])
    axs[1].plot(int_dates[period], accumulated)


def plot_sync_async_comparison(detailed: pd.Series,
                               synced: pd.Series,
                               asynced: pd.Series) -> None:
    fig, axs = plt.subplots(1, 2, figsize=(13, 5))

    detailed_dates = datetime_index_to_ints(detailed.index)
    synced_dates = datetime_index_to_ints(synced.index)
    asynced_dates = datetime_index_to_ints(asynced.index)

    axs[0].plot(detailed_dates, detailed.values, label='detailed')
    axs[0].plot(synced_dates, synced.values, '--o', label='synced')
    axs[0].legend(loc='best')
    axs[0].set_title('detailed vs synced')

    axs[1].plot(detailed_dates, detailed.values, label='detailed')
    axs[1].plot(asynced_dates, asynced.values, '--o', label='asynced')
    axs[1].legend(loc='best')
    axs[1].set_title('detailed vs asynced')
