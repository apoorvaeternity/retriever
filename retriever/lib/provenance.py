import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from tempfile import NamedTemporaryFile
from zipfile import ZipFile

from retriever import datasets, download
from retriever.lib.defaults import HOME_DIR, VERSION
from retriever.lib.engine_tools import getmd5

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
    info['md5'] = getmd5(os.path.join(HOME_DIR, 'raw_data', dataset.name), 'dir')
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
    if dataset.name not in os.listdir(raw_dir) and force_download:
        if not quiet:
            print("Dataset not in downloaded datasets. Downloading it.")
        download(dataset.name)
        data_exists = True

    elif dataset.name in os.listdir(raw_dir):
        data_exists = True
    if data_exists:
        for root, _, files in os.walk(os.path.join(raw_dir, dataset.name)):
            for file in files:
                paths_to_zip['raw_data'].append(os.path.join(root, file))
        info = commit_info(dataset)
        with ZipFile(os.path.join(path, '{}-{}'.format(dataset.name, info['md5'][:7])), 'w') as zipped:
            zipped.write(paths_to_zip['script'],
                         os.path.join('script', os.path.basename(paths_to_zip['script'])))
            for data_file in paths_to_zip['raw_data']:
                zipped.write(data_file, os.path.join(data_file.lstrip(raw_dir).rstrip(os.path.basename(data_file)),
                                                     os.path.basename(data_file)))
            metadata_temp_file = NamedTemporaryFile()
            with open(os.path.abspath(metadata_temp_file.name), 'w') as json_file:
                json.dump(info, json_file, sort_keys=True, indent=4)
            zipped.write(os.path.abspath(metadata_temp_file.name), 'metadata.json')
            metadata_temp_file.close()


    else:
        if not quiet:
            print("Dataset unavailable in downloaded datasets.")


if __name__ == '__main__':

    for dataset in datasets():
        if dataset.name == 'gentry-forest-transects':
            print(commit_info(dataset))
            print(commit(dataset, path='/home/apoorva/Desktop', force_download=True, quiet=True))
