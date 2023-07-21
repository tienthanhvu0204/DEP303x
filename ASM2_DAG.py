from __future__ import print_function
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime, timedelta
#import os.path
#from google_drive_downloader_module import GoogleDriveDownloader as gdd

import requests
import zipfile
import warnings
from sys import stdout
from os import makedirs
from os.path import dirname, isfile, exists
#from os.path import exists

class GoogleDriveDownloader:
    """
    Minimal class to download shared files from Google Drive.
    """

    CHUNK_SIZE = 32768
    DOWNLOAD_URL = 'https://docs.google.com/uc?export=download'

    @staticmethod
    def download_file_from_google_drive(file_id, dest_path, overwrite=False, unzip=False, showsize=False):
        """
        Downloads a shared file from google drive into a given folder.
        Optionally unzips it.

        Parameters
        ----------
        file_id: str
            the file identifier.
            You can obtain it from the sharable link.
        dest_path: str
            the destination where to save the downloaded file.
            Must be a path (for example: './downloaded_file.txt')
        overwrite: bool
            optional, if True forces re-download and overwrite.
        unzip: bool
            optional, if True unzips a file.
            If the file is not a zip file, ignores it.
        showsize: bool
            optional, if True print the current download size.
        Returns
        -------
        None
        """

        destination_directory = dirname(dest_path)
        if not exists(destination_directory):
            makedirs(destination_directory)

        if not exists(dest_path) or overwrite:

            session = requests.Session()

            print('Downloading {} into {}... '.format(file_id, dest_path), end='')
            stdout.flush()

            response = session.post(GoogleDriveDownloader.DOWNLOAD_URL, params={'id': file_id, 'confirm': 't'}, stream=True)

            if showsize:
                print()  # Skip to the next line

            current_download_size = [0]
            GoogleDriveDownloader._save_response_content(response, dest_path, showsize, current_download_size)
            print('Done.')

            if unzip:
                try:
                    print('Unzipping...', end='')
                    stdout.flush()
                    with zipfile.ZipFile(dest_path, 'r') as z:
                        z.extractall(destination_directory)
                    print('Done.')
                except zipfile.BadZipfile:
                    warnings.warn('Ignoring `unzip` since "{}" does not look like a valid zip file'.format(file_id))

    @staticmethod
    def _save_response_content(response, destination, showsize, current_size):
        with open(destination, 'wb') as f:
            for chunk in response.iter_content(GoogleDriveDownloader.CHUNK_SIZE):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
                    if showsize:
                        print('\r' + GoogleDriveDownloader.sizeof_fmt(current_size[0]), end=' ')
                        stdout.flush()
                        current_size[0] += GoogleDriveDownloader.CHUNK_SIZE

    # From https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    @staticmethod
    def sizeof_fmt(num, suffix='B'):
        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return '{:.1f} {}{}'.format(num, unit, suffix)
            num /= 1024.0
        return '{:.1f} {}{}'.format(num, 'Yi', suffix)

default_args = {
    "start_date": datetime(2020, 1, 1)
}

#ham branching, phan nhanh stask theo dieu kien
def _branching():
    #kiem tra file co ton tai khong
    answers_path = "./download/Answers.csv"
    questions_path = "./download/Questions.csv"
    check_cond = isfile(answers_path) and isfile(questions_path)
    #phan nhanh theo dieu kien 
    if check_cond == True:
        return 'end'
    else:
        return 'clear_file'

#Ham download file tu google drive
def _download_file(_id, path):
    gdd = GoogleDriveDownloader()
    gdd.download_file_from_google_drive(file_id=_id,
                                    dest_path=path)


def _spark_process ():
    pass

with DAG(
    'dag_asm2',
    default_args = default_args,
    schedule_interval = "@daily",
    catchup = False
    ) as dag:

    start = DummyOperator(
        task_id = 'start'
    )

    end = DummyOperator(
        task_id = 'end'
    )

    branching = BranchPythonOperator(
        task_id = 'branching',
        python_callable = _branching
    )

    clear_file = BashOperator(
        task_id = 'clear_file',
        bash_command = 'rm -rf ./download'
    )

    download_questions = PythonOperator(
        task_id = 'download_questions_file',
        python_callable = _download_file,
        op_kwargs = {'_id': '1bVO0izsJcdxbq2z4Zsc2bw_4pFxWCnk1', 'path': './download/Questions.csv'}
    )

    download_answers = PythonOperator(
        task_id = 'download_answers_file',
        python_callable = _download_file,
        op_kwargs = {'_id': '1w_i3AxkjH_qqa186M_M-WMSVDq3MzdgF', 'path': './download/Answers.csv'}
    )

    import_questions = BashOperator(
        task_id = 'import_questions_mongo',
        bash_command = 'mongoimport --type csv -d airflow_asm2 -c Questions --headerline --drop ./download/Questions.csv'
    )

    import_answers = BashOperator(
        task_id = 'import_answers_mongo',
        bash_command = 'mongoimport --type csv -d airflow_asm2 -c Answers --headerline --drop ./download/Answers.csv'

    )

    spark_process = PythonOperator(
        task_id = 'spark_process',
        python_callable = _spark_process
    )

    import_output = BashOperator(
        task_id = 'import_output_mongo',
        bash_command = 'mongoimport --type csv -d airflow_asm2 -c Result --headerline --drop ./data_output'
    )

    start >> branching >> [clear_file, end]
    clear_file >> [download_questions, download_answers] 
    download_questions >> import_questions
    download_answers >> import_answers 
    [import_questions, import_answers] >> spark_process >> import_output >> end 



