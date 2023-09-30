from src.utils.time_script import time_stamp

from multiprocess_load import load_file_multiprocces


@time_stamp
def load_all_data(data_mapping):
    for data_path, model in data_mapping.items():
        load_file_multiprocces(data_path, model)
