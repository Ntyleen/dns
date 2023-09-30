from table_class import Sales, Products, Cities, Branches
from mapping import data_mapping
from create_tables import create_tables
from data_loader_multiprocess import load_all_data



tables = [Cities, Products, Branches, Sales]

if __name__ == '__main__':
    create_tables(tables)
    load_all_data(data_mapping)
