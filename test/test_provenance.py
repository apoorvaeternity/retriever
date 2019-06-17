import os
import pytest
from shutil import rmtree
from tempfile import mkdtemp

from retriever.engines import engine_list
from retriever.lib.load_json import read_json
from retriever.lib.provenance import commit

file_location = os.path.normpath(os.path.dirname(os.path.realpath(__file__)))
mysql_engine, postgres_engine, sqlite_engine, msaccess_engine, \
csv_engine, download_engine, json_engine, xml_engine = engine_list

test_commit_details = [('dataset_provenance', {
    'main': 'https://github.com/apoorvaeternity/sample-dataset/raw/master/modified/dataset_provenance.csv'},
                        {'original': 'dataset-provenance-396e22.zip', 'modified': 'dataset-provenance-a69e22.zip'})]


def get_script_module(script_name):
    """Load a script module."""
    return read_json(os.path.join(file_location, script_name))


def install_and_commit(script_module, test_dir, commit_message, modified_table_urls={}):
    for table in modified_table_urls:
        # modify the url to the csv file
        setattr(script_module.tables[table], 'url', modified_table_urls[table])
    sqlite_engine.opts = {'install': 'sqlite', 'file': 'test_db.sqlite3', 'table_name': '{db}_{table}',
                          'data_dir': '.'}
    sqlite_engine.use_cache = False
    # check if dataset is installing properly
    script_module.download(engine=sqlite_engine)
    script_module.engine.final_cleanup()
    commit(script_module, path=test_dir, commit_message=commit_message)


@pytest.mark.parametrize("script_file_name, modified_table_urls, expected_archives", test_commit_details)
def test_commit(script_file_name, modified_table_urls, expected_archives):
    test_dir = mkdtemp(dir=os.path.dirname(os.path.realpath(__file__)))
    os.chdir(test_dir)
    script_module = get_script_module(os.path.join('raw_data/scripts/', script_file_name))
    setattr(script_module, "_file",
            os.path.join(file_location, 'raw_data/scripts/', '{}.json'.format(script_file_name)))
    setattr(script_module, "_name", script_file_name.replace('_', '-'))
    # install original version
    install_and_commit(script_module, test_dir=test_dir, commit_message="Original")
    # install modified version
    install_and_commit(script_module, test_dir=test_dir, commit_message="Modified",
                       modified_table_urls=modified_table_urls)
    # check if the required archive files exist
    original_archive_exist = True if os.path.isfile(
        os.path.join(test_dir, expected_archives['original'])) else False
    modified_archive_exist = True if os.path.isfile(
        os.path.join(test_dir, expected_archives['modified'])) else False
    os.chdir(file_location)
    rmtree(test_dir)

    assert original_archive_exist == True
    assert modified_archive_exist == True
