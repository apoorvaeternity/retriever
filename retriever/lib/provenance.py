import os
from datetime import datetime, timezone
from zipfile import ZipFile
from pip._internal.operations.freeze import freeze

from retriever import datasets
from retriever.lib.defaults import HOME_DIR, VERSION


def commit_info():
    info = {}
    info['packages'] = {'retriever': VERSION}
    info['time'] = datetime.now(timezone.utc).strftime("%m/%d/%Y, %H:%M:%S")

    for package in freeze(exclude_editable=True):
        print(package)
        package_name, version = package.split('==')
        info['packages'][package_name] = version
    return info


def commit(dataset, path=None):
    """
    Commit dataset to a zipped file.
    """
    paths_to_zip = {}
    paths_to_zip['script'] = dataset._file
    paths_to_zip['raw_data'] = []

    raw_dir = os.path.join(HOME_DIR, 'raw_data', dataset.name)

    for file in os.listdir(os.path.join(raw_dir)):
        paths_to_zip['raw_data'].append(os.path.join(raw_dir, file))

    with ZipFile(os.path.join(path, dataset.name), 'w') as zipped:
        zipped.write(paths_to_zip['script'],
                     os.path.join('script', os.path.basename(paths_to_zip['script'])))
        for data_file in paths_to_zip['raw_data']:
            zipped.write(data_file, os.path.join('raw_data', os.path.basename(data_file)))


if __name__ == '__main__':
    print(commit_info())
    # for dataset in datasets():
    #     if dataset.name == 'abalone-age':
    #         print(commit(dataset, path='/home/apoorva/Desktop'))
