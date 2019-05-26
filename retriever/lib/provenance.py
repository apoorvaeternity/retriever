import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from tempfile import NamedTemporaryFile, mkdtemp
from zipfile import ZipFile

from retriever import datasets
from retriever.engines import choose_engine
from retriever.lib.defaults import HOME_DIR, VERSION
from retriever.lib.engine_tools import getmd5
from retriever.lib.load_json import read_json
from retriever.engines import engine_list
sqlite_engine = [eng for eng in engine_list if eng.name == 'CSV'][0]
def package_details():
    details = {}
    details['retriever'] = VERSION[1:]
    packages = subprocess.check_output([sys.executable, '-m', 'pip',
                                        'freeze', '--exclude-editable']).decode("utf-8").split('\n')
    for package in packages:
        if package:
            package_name, version = package.split('==')
            details[package_name] = version
    return details


def commit_info(dataset):
    """
    Generate info for a particular commit.
    """
    info = {}
    info['packages'] = package_details()
    info['time'] = datetime.now(timezone.utc).strftime("%m/%d/%Y, %H:%M:%S")
    info['version'] = dataset.version
    return info


def commit(dataset, path=None, force_download=False, quiet=False):
    """
    Commit dataset to a zipped file.
    """
    paths_to_zip = {}
    paths_to_zip['script'] = dataset._file
    paths_to_zip['raw_data'] = []
    raw_dir = os.path.join(HOME_DIR, 'raw_data')
    data_exists = False
    if dataset.name not in os.listdir(raw_dir) or force_download:
        if not quiet:
            print("Downloading dataset.")
        if dataset._file.endswith('.py'):
            dataset.download(engine=choose_engine({'command': 'download', 'path': './', 'sub_dir': ""}))
        else:
            dataset.download(engine=choose_engine({'command': 'download', 'path': './',
                                                   'sub_dir': ""}, choice=False))
        data_exists = True

    elif dataset.name in os.listdir(raw_dir):
        data_exists = True

    if data_exists:
        for root, _, files in os.walk(os.path.join(raw_dir, dataset.name)):
            for file in files:
                paths_to_zip['raw_data'].append(os.path.join(root, file))

        info = commit_info(dataset)
        info['name'] = dataset.name
        if os.path.exists(os.path.join(HOME_DIR, 'raw_data', dataset.name)):
            info['md5'] = getmd5(os.path.join(HOME_DIR, 'raw_data', dataset.name), 'dir')

        with ZipFile(os.path.join(path, '{}-{}'.format(dataset.name, info['md5'][:7])), 'w') as zipped:
            zipped.write(paths_to_zip['script'],
                         os.path.join('script', os.path.basename(paths_to_zip['script'])))

            for data_file in paths_to_zip['raw_data']:
                zipped.write(data_file, data_file.split(raw_dir)[1])

            metadata_temp_file = NamedTemporaryFile()
            with open(os.path.abspath(metadata_temp_file.name), 'w') as json_file:
                json.dump(info, json_file, sort_keys=True, indent=4)
            zipped.write(os.path.abspath(metadata_temp_file.name), 'metadata.json')
            metadata_temp_file.close()


    else:
        if not quiet:
            print("Dataset unavailable in downloaded datasets.\n"
                  "To commit the dataset either download it or enable force download.")


def install_committed(path_to_archive):
    with ZipFile(path_to_archive, 'r') as archive:
        commit_details = json.loads(archive.read('metadata.json').decode('utf-8'))
        workdir = mkdtemp(dir=os.path.dirname(path_to_archive))
        archive.extractall(workdir)
        script_object = read_json(os.path.join(workdir, 'script', commit_details['name']))
        print(script_object)
        # engine = sqlite_engine.__new__(sqlite_engine.__class__)
        # engine.script_table_registry = {}
        # args = {
        #     'command': 'install',
        #     'dataset': script_object,
        #     'file':'new_file.db',
        #     'table_name': '{db}_{table}',
        #     'data_dir':'.'
        # }
        # engine.opts = args
        # engine.use_cache = False
        # script_object.download(engine=engine, debug=True)



if __name__ == '__main__':
    install_committed('/home/apoorva/Desktop/gdp-0048bc8')
    # for dataset in datasets():
    #     if dataset.name == 'gdp':
    #         print(commit_info(dataset))
    #         print(commit(dataset, path='/home/apoorva/Desktop', force_download=True, quiet=False))
