import pandas as pd
from pandas import HDFStore
from nilmtk import DataSet

from utils.constants import DatasetType
from utils.preprocessing import clear_nulls
from utils.timing import time_measure


def convert_to_nilmtk_format(ds: DatasetType) -> None:
    with time_measure(f'converting {ds.name} dataset to nilmtk format'):
        ds.nilmtk_converter(ds.archive_path(), ds.nilmtk_path())


def process_data(ds: DatasetType) -> None:
    with time_measure(f'processing and storing {ds.name} dataset from nilmtk format'):
        nilmtk_data = DataSet(ds.nilmtk_path())
        keys = nilmtk_data.buildings.keys()
        processed_data_file = HDFStore(ds.processed_path(), mode='w', complevel=5)

        for key in keys:
            elec = nilmtk_data.buildings[key].elec
            data = elec.mains().power_series_all_data()
            data = clear_nulls(data)
            processed_data_file['building_' + str(key)] = data

        nilmtk_data.store.close()
        processed_data_file.close()


def read_processed_data(ds: DatasetType) -> dict[str, pd.Series]:
    with time_measure(f'reading processed {ds.name} dataset'):
        data = dict()
        data_file = HDFStore(ds.processed_path(), mode='r')
        for key in data_file.keys():
            data[key] = data_file[key]
        data_file.close()
        return data
