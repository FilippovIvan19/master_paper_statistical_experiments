from utils.constants import DatasetType, DAY_IN_SEC
from utils.data_reading import clean_and_store_data, convert_to_nilmtk_format, \
    process_stable_periods


# this file purpose is to demonstrate data preparation flow

# setting params for data preprocessing
DS = DatasetType.IDEAL
DURATION = DAY_IN_SEC*30
MAX_GAP = 300

# was used only for IDEAL because REDD was gotten in already converted format
convert_to_nilmtk_format(DS)

# was used to delete nulls and incorrect (negative and infinite) values
clean_and_store_data(DS)

# was used to get stable periods, interpolate gaps and store
# data in power and accumulated formats
# it takes some time for computations to complete
process_stable_periods(DS, DURATION, MAX_GAP)
