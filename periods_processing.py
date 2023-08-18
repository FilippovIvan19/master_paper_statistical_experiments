from utils.constants import DatasetType, DAY_IN_SEC, DURATION_MAX_GAP_PAIRS
from utils.data_reading import process_stable_periods, get_full_keys_of_stable_periods


if __name__ == '__main__':
    DS = DatasetType.IDEAL
    for dur, gap in DURATION_MAX_GAP_PAIRS[0:1]:
        print(dur//DAY_IN_SEC, gap)
        process_stable_periods(DS, dur, gap)
        print(len(get_full_keys_of_stable_periods(DS)))
