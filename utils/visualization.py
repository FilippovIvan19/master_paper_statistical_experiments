import matplotlib.pyplot as plt
import itertools
import operator
import numpy as np
import pandas as pd


def plot_power_and_consumption(data: pd.Series, period: slice = slice(-1)) -> None:
    int_dates = data.index.astype(np.int64) // 10 ** 9
    accumulated = list(itertools.accumulate(data.values[period], operator.add))
    fig, axs = plt.subplots(1, 2, figsize=(13, 5))
    axs[0].plot(int_dates[period], data.values[period])
    axs[1].plot(int_dates[period], accumulated)
