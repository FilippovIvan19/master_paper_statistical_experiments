import numpy as np
import pandas as pd
from scipy.stats import gmean
import os

from utils.constants import EXP_RESULTS_PATH, DatasetType, ACCUMULATED_SHEET, POWER_SHEET
from nilmtk import STATS_CACHE


STATS_CACHE.store.close()


SYNC_ASYNC_RATIO = 'sync_async_ratio'


def comp_async_win_percentage(ratios: np.ndarray):
    win_count = list(filter(lambda x: x > 1, ratios))
    return len(ratios), gmean(ratios), len(win_count) / len(ratios) * 100


def analyze_sync_async_ratio(ds, source_sheet, dist_type, grouping_param):
    source_file = EXP_RESULTS_PATH + 'united_' + ds.path_str + '.xlsx'
    data = pd.read_excel(source_file, source_sheet)
    percentage = data \
        .groupby(grouping_param)[[SYNC_ASYNC_RATIO]] \
        .agg(comp_async_win_percentage)

    result = pd.DataFrame(percentage[SYNC_ASYNC_RATIO].tolist(),
                          index=percentage.index,
                          columns=['count', 'geometric_mean', 'win_percentage'])

    result_file = EXP_RESULTS_PATH + 'processed_results.xlsx'
    mode = 'a' if os.path.isfile(result_file) else 'w'
    with pd.ExcelWriter(result_file, mode=mode) as writer:
        result.to_excel(writer, sheet_name=f'{ds.path_str}_{dist_type}__by_{grouping_param}')
        print(f'{ds.path_str}_{dist_type}__by_{grouping_param}')


for ds in [DatasetType.IDEAL, DatasetType.REDD]:
    for source_sheet, dist_type in ((ACCUMULATED_SHEET, 'accum'), (POWER_SHEET, 'power')):
        for grouping_param in ['key', 'duration', 'max_gap', 'time_delta']:
            analyze_sync_async_ratio(ds, source_sheet, dist_type, grouping_param)
