import numpy as np
import pandas as pd
from numpy.polynomial.polynomial import Polynomial
from scipy.interpolate import InterpolatedUnivariateSpline, splrep, PPoly
from multiprocessing.pool import ThreadPool, Pool

from utils.preprocessing import datetime_index_to_floats
from utils.timing import time_measure


def calc_squared_coefs(interpolations, i):
    p = Polynomial(interpolations.c[:, i][::-1]) / 100
    p_squared = p ** 2
    squared_coefs = p_squared.coef[::-1]
    squared_coefs = np.pad(squared_coefs, (0, 3 - len(squared_coefs)))
    return squared_coefs


def calc_squared_coefs_range(interpolations, start, end):
    with time_measure(f'calc_squared_coefs_range from {start} to {end}', is_active=False):
        return [
            calc_squared_coefs(interpolations, i)
            for i in range(start, end)
        ]


def accumulated_distance(original: pd.Series, generated: pd.Series, parallel: int = 1):
    with time_measure(f'accumulated_distance'):
        f_dates_orig = datetime_index_to_floats(original.index)
        f_dates_gen = datetime_index_to_floats(generated.index)
        united_dates = f_dates_orig.union(f_dates_gen)
        point_count = len(united_dates)

        lin_orig = InterpolatedUnivariateSpline(f_dates_orig, original.values, k=1)
        lin_gen = InterpolatedUnivariateSpline(f_dates_gen, generated.values, k=1)
        all_values_orig = lin_orig(united_dates)
        all_values_gen = lin_gen(united_dates)
        diff = all_values_orig - all_values_gen

        tck = splrep(united_dates, diff, k=1, s=0)
        interpolations = PPoly.from_spline(tck)

        assert interpolations.c.shape[0] == 2, 'must be linear'
        assert interpolations.c.shape[1] == point_count + 1, 'interval count is wrong'

        if parallel == 1:
            squared_coefs = calc_squared_coefs_range(interpolations, 0, point_count + 1)
            squared_coefs = np.stack(squared_coefs, axis=0)
        else:
            chunk_size = point_count // parallel + 1
            arguments = [
                (interpolations, i, min(i+chunk_size, point_count+1))
                for i in range(0, point_count + 1, chunk_size)
            ]
            with Pool(parallel) as pool:
                squared_coefs_parts = pool.starmap(calc_squared_coefs_range, arguments)
            squared_coefs = np.concatenate(squared_coefs_parts, axis=0)

        squared_diffs = PPoly(squared_coefs.T, interpolations.x)

        distance = squared_diffs.integrate(united_dates[0], united_dates[-1])
        print('distance', distance)
        return distance


def consumption_rate_distance():
    pass


