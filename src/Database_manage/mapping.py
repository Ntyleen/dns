from table_class import Products, Cities, Branches, Sales

data_mapping = {
    "../data/t_products.csv": Products,
    "../data/t_cities.csv": Cities,
    "../data/t_branches.csv": Branches,
    "../data/t_sales.csv": Sales
}

column_mapping = {
    "get_top_sales_hour_and_day": ["День_недели", "Час", "Количество_продаж"],
    "get_top_cities_by_sales": ["Город", "Количество_продаж"],
    "get_top_branches_by_sales": ["Название_филиала", "Количество_продаж"],
    "get_top_products_by_sales_at_branches": ["Название_товара", "Количество_продаж"],
    "get_top_products_by_sales_at_storages": ["Название_товара", "Количество_продаж"],
    "get_top_branches_by_sales_value": ["Название_филиала", "Количество_продаж"],
    "get_sales_by_day": ["День_недели", "Количество_продаж"],
    "get_sales_by_hour": ["Час", "Количество_продаж"],
    "classify_products": ["Номенклатура", "Объем_продаж"]
}



