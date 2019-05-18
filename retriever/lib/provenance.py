import os
import subprocess
import sys
from datetime import datetime, timezone
from zipfile import ZipFile

from retriever import datasets, download
from retriever.lib.defaults import HOME_DIR, VERSION


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


def commit_info():
    """
    Generate info for a particular commit.
    """
    info = {}
    info['packages'] = package_details()
    info['time'] = datetime.now(timezone.utc).strftime("%m/%d/%Y, %H:%M:%S")
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
        for root, directory, files in os.walk(os.path.join(raw_dir, dataset.name)):
            for file in files:
                paths_to_zip['raw_data'].append(os.path.join(root, file))

        with ZipFile(os.path.join(path, dataset.name), 'w') as zipped:
            zipped.write(paths_to_zip['script'],
                         os.path.join('script', os.path.basename(paths_to_zip['script'])))
            for data_file in paths_to_zip['raw_data']:
                zipped.write(data_file, os.path.join(data_file.lstrip(raw_dir).rstrip(os.path.basename(data_file)),
                                                     os.path.basename(data_file)))
    else:
        if not quiet:
            print("Dataset unavailable in downloaded datasets.")


if __name__ == '__main__':
    print(commit_info())
    for dataset in datasets():
        if dataset.name == 'gentry-forest-transects':
            print(commit(dataset, path='/home/apoorva/Desktop', force_download=True, quiet=True))
