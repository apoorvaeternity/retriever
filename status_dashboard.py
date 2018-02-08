from retriever import install_sqlite
from retriever.lib.engine_tools import getmd5
from retriever import datasets
from retriever.engines import engine_list

mysql_engine, postgres_engine, sqlite_engine, msaccess_engine, csv_engine, download_engine, json_engine, xml_engine = engine_list

example_datasets = ['bird-size', 'mammal-masses', 'airports']
for script in datasets():
    if script.name in example_datasets:
        install_sqlite(script.name, use_cache=False, debug=True)
        engine_obj = script.checkengine(sqlite_engine)
        engine_obj.to_csv()
        for table_n in script.tables.keys():
            print(getmd5("{}_{}.csv".format(script.name.replace("-","_"),table_n),data_type='file'))
