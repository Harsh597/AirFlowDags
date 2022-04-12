from csv import DictReader
import os
import pandas as pd














































from datetime import date, datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable


def read_csv():
    # dt = ti.xcom_pull(task_ids=['get_datetime'])
    df = pd.read_csv(r'/home/hgupta/Desktop/DataIn/Dob.csv')
    df['Timestamp'] = ""
    df.to_csv(r'/home/hgupta/Desktop/DataIn/Dob_ts.csv', index=False)

    # row_num = 0
    # rows = len(df.index)
    # for row in df:
    #     if (row_num < rows):
    #         df.loc[row_num, 'Timestamp'] = str(datetime.today())
    #     row_num += 1
    #
    # df.to_csv(r'/home/hgupta/Desktop/DataIn/Dob_ts.csv', index=False)
    return 'Reading DOB.csv file is successfull'


def add_timest():
    df = pd.read_csv(r'/home/hgupta/Desktop/DataIn/Dob_ts.csv')
    row_num = 0
    rows = len(df.index)
    for row in df:
        if row_num < rows:
            df.loc[row_num, 'Timestamp'] = str(datetime.today())
        row_num += 1
    df.to_csv(r'/home/hgupta/Desktop/DataIn/Dob_ts.csv', index=False)

    return 'Added Timestamp to each column'

with DAG(
        dag_id='first_dag',
        # schedule_interval=None,
        start_date=datetime(year=2022, month=4, day=6),
        catchup=False
) as dag:
    # 1 Read CSV file
    t1 = PythonOperator(
        task_id='get_csv',
        python_callable=read_csv
    )
    # 2 Adding Timestamp col to csv
    t2 = PythonOperator(
        task_id='add_ts',
        python_callable=add_timest
    )
    t1 >> t2
