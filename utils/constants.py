import enum
import re
from typing import Callable

from nilmtk.dataset_converters import convert_redd
from utils.nilmtk_additional import convert_ideal


DAY_IN_SEC = 60 * 60 * 24
WEEK_IN_SEC = 7 * DAY_IN_SEC
MONTH_IN_SEC = 30 * DAY_IN_SEC

DATASET_NAMES = ['redd', 'ideal']

ARCHIVES_PATH = '../data/unpacked/archive_storage/'
NILMTK_PATH = '../data/nilmtk_storage/'
CLEANED_PATH = '../data/cleaned_data_storage/'
PERIODS_PATH = '../data/periods_storage/'
POWER_PERIODS_PATH = '../data/power_periods_storage/'


class DatasetType(enum.Enum):
    REDD = ('redd', convert_redd)
    IDEAL = ('ideal', convert_ideal)

    def __init__(self, path_str: str, nilmtk_converter: Callable):
        self.path_str: str = path_str
        self.nilmtk_converter: Callable = nilmtk_converter

    def archive_path(self) -> str:
        return ARCHIVES_PATH + self.path_str

    def nilmtk_path(self) -> str:
        return NILMTK_PATH + self.path_str + '.h5'

    def cleaned_path(self) -> str:
        return CLEANED_PATH + self.path_str + '.h5'

    def periods_path(self, power_mode: bool = False) -> str:
        if power_mode:
            return POWER_PERIODS_PATH + self.path_str + '.h5'
        return PERIODS_PATH + self.path_str + '.h5'


FULL_KEY_PATTERN = re.compile(r'/(\w+)/dur_(\w+)/gap_(\w+)/n_(\w+)')


DURATION_MAX_GAP_PAIRS = [
    (DAY_IN_SEC * 30, 300),
    (DAY_IN_SEC * 20, 300),
    (DAY_IN_SEC * 15, 300),
    (DAY_IN_SEC * 10, 300),

    (DAY_IN_SEC * 20, 180),
    (DAY_IN_SEC * 15, 180),
    (DAY_IN_SEC * 10, 180),

    (DAY_IN_SEC * 15, 90),
    (DAY_IN_SEC * 10, 90),
    (DAY_IN_SEC * 5, 90),

    (DAY_IN_SEC * 14, 60),
    (DAY_IN_SEC * 10, 60),
    (DAY_IN_SEC * 7, 60),
    (DAY_IN_SEC * 5, 60),

    (DAY_IN_SEC * 10, 30),
    (DAY_IN_SEC * 7, 30),
    (DAY_IN_SEC * 5, 30),
    (DAY_IN_SEC * 3, 30),

    (DAY_IN_SEC * 7, 15),
    (DAY_IN_SEC * 5, 15),
    (DAY_IN_SEC * 3, 15),

    (DAY_IN_SEC * 5, 10),
    (DAY_IN_SEC * 3, 10),
    (DAY_IN_SEC, 10),

    (DAY_IN_SEC * 5, 5),
    (DAY_IN_SEC * 3, 5),
    (DAY_IN_SEC, 5),

    (DAY_IN_SEC * 5, 1),
    (DAY_IN_SEC * 3, 1),
    (DAY_IN_SEC, 1),
]

TIME_DELTAS = [
    3, 5, 10, 15, 20, 30,
    60, 120, 240, 600
]
