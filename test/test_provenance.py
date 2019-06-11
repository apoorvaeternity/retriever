import os
from shutil import rmtree
from tempfile import mkdtemp

from retriever.engines import engine_list
from retriever.lib.load_json import read_json
from retriever.lib.provenance import commit

file_location = os.path.normpath(os.path.dirname(os.path.realpath(__file__)))
modified_dataset_path = "https://github.com/apoorvaeternity/sample-dataset/raw/master/modified/Portal_rodents_19772002.csv"
mysql_engine, postgres_engine, sqlite_engine, msaccess_engine, \
csv_engine, download_engine, json_engine, xml_engine = engine_list

# Note: The hash in the archive name is the concatenation of first 3 characters of md5 value of
# raw data and that of the script file
original_archive = 'sample-dataset-3960d3.zip'  # the archive file with original version of dataset
modified_archive = 'sample-dataset-a690d3.zip'  # the archive file with modified version of dataset


def get_script_module(script_name):
    """Load a script module."""
    return read_json(os.path.join(file_location, script_name))


def test_commit():
    test_dir = mkdtemp(dir=os.path.dirname(os.path.realpath(__file__)))
    os.chdir(test_dir)
    script_module = get_script_module(os.path.join('raw_data/scripts/', 'sample_dataset'))
    setattr(script_module, "_file", os.path.join(file_location, 'raw_data/scripts/sample_dataset.json'))
    setattr(script_module, "_name", 'sample_dataset')
    sqlite_engine.opts = {'install': 'sqlite', 'file': 'test_db.sqlite3', 'table_name': '{db}_{table}',
                          'data_dir': '.'}
    sqlite_engine.use_cache = False
    # check if dataset is installing properly
    script_module.download(engine=sqlite_engine)
    script_module.engine.final_cleanup()
    commit(script_module, path=test_dir, commit_message="Original")
    # modify the url to the csv file
    setattr(script_module.tables['main'], 'url', modified_dataset_path)
    sqlite_engine.opts = {'install': 'sqlite', 'file': 'test_db2.sqlite3', 'table_name': '{db}_{table}',
                          'data_dir': '.'}
    sqlite_engine.use_cache = False
    # download the modified version of dataset
    script_module.download(engine=sqlite_engine)
    script_module.engine.final_cleanup()
    commit(script_module, path=test_dir, commit_message="Modified")

    # check if the required archive files exist
    original_archive_exist = True if os.path.isfile(
        os.path.join(test_dir, original_archive)) else False
    modified_archive_exist = True if os.path.isfile(
        os.path.join(test_dir, modified_archive)) else False
    os.chdir(file_location)
    rmtree(test_dir)

    assert original_archive_exist == True
    assert modified_archive_exist == True
