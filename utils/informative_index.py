import numpy as np
import pandas as pd
from numpy.polynomial.polynomial import Polynomial
from scipy.interpolate import InterpolatedUnivariateSpline, splrep, PPoly

from utils.preprocessing import datetime_index_to_floats
from utils.timing import time_measure


def accumulated_distance(original: pd.Series, generated: pd.Series):
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

        squared_coefs = np.empty((3, point_count + 1), dtype=np.float64)
        for i in range(point_count + 1):
            p = Polynomial(interpolations.c[:, i][::-1]) / 100
            p_squared = p ** 2
            coefs_to_apply = p_squared.coef[::-1]
            coefs_to_apply = np.pad(coefs_to_apply, (0, 3 - len(coefs_to_apply)))
            for ind, c in enumerate(coefs_to_apply):
                squared_coefs[ind, i] = c

        squared_diffs = PPoly(squared_coefs, interpolations.x)

        distance = squared_diffs.integrate(united_dates[0], united_dates[-1])
        print('distance')
        print(distance)
        return distance


def consumption_rate_distance():
    pass


