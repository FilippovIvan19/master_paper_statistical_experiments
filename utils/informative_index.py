import numpy as np
import pandas as pd
from numpy.polynomial.polynomial import Polynomial
from scipy.interpolate import InterpolatedUnivariateSpline, splrep, PPoly, interp1d
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


def integrate_squared_diff(piecewise_polynomials: PPoly, processes: int = 1) -> float:
    point_count = len(piecewise_polynomials.x) - 2

    if processes == 1:
        squared_coefs = calc_squared_coefs_range(piecewise_polynomials, 0, point_count + 1)
        squared_coefs = np.stack(squared_coefs, axis=0)
    else:
        chunk_size = point_count // processes + 1
        arguments = [
            (piecewise_polynomials, i, min(i + chunk_size, point_count + 1))
            for i in range(0, point_count + 1, chunk_size)
        ]
        with Pool(processes) as pool:
            squared_coefs_parts = pool.starmap(calc_squared_coefs_range, arguments)
        squared_coefs = np.concatenate(squared_coefs_parts, axis=0)

    squared_diffs = PPoly(squared_coefs.T, piecewise_polynomials.x)
    distance = squared_diffs.integrate(piecewise_polynomials.x[0], piecewise_polynomials.x[-1])
    return distance.item()


def accumulated_distance(original: pd.Series, generated: pd.Series, processes: int = 1) -> float:
    with time_measure(f'accumulated_distance', is_active=False):
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

        distance = integrate_squared_diff(interpolations, processes)
        return distance


def power_distance(original_pow: pd.Series, generated_acc: pd.Series, processes: int = 1) -> float:
    with time_measure(f'power_distance', is_active=False):
        f_dates_orig = datetime_index_to_floats(original_pow.index)
        f_dates_gen = datetime_index_to_floats(generated_acc.index)
        united_dates = f_dates_orig.union(f_dates_gen)[1:]

        lin_gen = InterpolatedUnivariateSpline(f_dates_gen, generated_acc.values, k=1)
        gen_der = lin_gen.derivative()

        step_orig = interp1d(
            f_dates_orig, original_pow.values, 'next', bounds_error=False,
            fill_value=(original_pow.values[0], original_pow.values[-1])
        )
        step_gen = interp1d(
            f_dates_gen[1:], gen_der(f_dates_gen[:-1]), 'next', bounds_error=False,
            fill_value=(gen_der(f_dates_gen[0]), gen_der(f_dates_gen[-2])))

        all_values_orig = step_orig(united_dates)
        all_values_gen = step_gen(united_dates)
        diff = all_values_orig - all_values_gen

        xs = united_dates.tolist()
        coefs = diff[1:]
        interpolations = PPoly([coefs], xs)

        distance = integrate_squared_diff(interpolations, processes)
        return distance

