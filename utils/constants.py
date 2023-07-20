import enum
from typing import Callable

from nilmtk.dataset_converters import convert_redd
from utils.nilmtk_additional import convert_ideal


DAY_IN_SEC = 60 * 60 * 24
WEEK_IN_SEC = 7 * DAY_IN_SEC
MONTH_IN_SEC = 30 * DAY_IN_SEC

DATASET_NAMES = ['redd', 'ideal']

ARCHIVES_PATH = '../data/unpacked/archive_storage/'
NILMTK_PATH = '../data/nilmtk_storage/'
PROCESSED_PATH = '../data/processed_data_storage/'
PERIODS_PATH = '../data/periods_storage/'


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

    def processed_path(self) -> str:
        return PROCESSED_PATH + self.path_str + '.h5'

    def periods_path(self) -> str:
        return PERIODS_PATH + self.path_str + '.h5'
