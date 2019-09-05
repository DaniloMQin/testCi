from __future__ import print_function
import json
import datetime
import logging
import google.cloud.logging
import subprocess
import time
import os
from airflow import configuration
from airflow import models
from airflow.contrib.hooks import gcs_hook
from airflow.contrib.operators import dataflow_operator
from airflow.operators import python_operator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage
from google.cloud.storage import Blob    
from airflow.operators import bash_operator
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow import AirflowException
from google.cloud import bigquery
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

# Import library
from dependencies import DPLF_Functions_Library as DPLF_Library

# DAG Args
v_yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {'start_date': v_yesterday}


def DPFL_DynamicDAG (**kwargs):
    DPLF_Config = DPLF_Library.DPLF_Access_ReadConfig()
    for p_task in DPLF_Config['GlobalVariables']:
        g_composer_bucket_path = DPLF_Library.DPLF_GetValueByKey(p_task, "g_composer_bucket_path")
    v_sql_path = g_composer_bucket_path + "/sql/"


    for p_task in DPLF_Config['Scheduling']:
        v_start_date = DPLF_Library.DPLF_GetValueByKey(p_task, "start_date")
        v_schedule_interval = DPLF_Library.DPLF_GetValueByKey(p_task, "schedule_interval")
        v_default_args = DPLF_Library.DPLF_GetValueByKey(p_task, "default_args")
        v_Dag_name = DPLF_Library.DPLF_GetValueByKey(p_task, "Dag_name")
        v_retries = DPLF_Library.DPLF_GetValueByKey(p_task, "retries")
        v_retry_delay = DPLF_Library.DPLF_GetValueByKey(p_task, "retry_delay")


    ###################
    #    Libraries    #
    ###################
    v_libraries = """
from __future__ import print_function
import json
import datetime
import logging
import google.cloud.logging
import subprocess
import time
import os
from airflow import configuration
from airflow import models
from airflow.contrib.hooks import gcs_hook
from airflow.contrib.operators import dataflow_operator
from airflow.operators import python_operator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage
from google.cloud.storage import Blob    
from airflow.operators import bash_operator
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow import AirflowException
from google.cloud import bigquery
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
# Import library 
from dependencies import DPLF_Functions_Library as DPLF_Library
\n"""


    ##################
    #    DAG Args    #
    ##################
    v_start_args = v_default_args+""" = {
    'start_date': datetime.datetime("""+v_start_date+"""),
    'retries':"""+v_retries+""",
    'retry_delay': datetime.timedelta(seconds="""+v_retry_delay+""")
    }
    \n"""


    ########################
    #    DAG Definition    #
    ########################
    v_TaskDag = """
with models.DAG('"""+v_Dag_name+"""',
                schedule_interval="""+v_schedule_interval+""",
                default_args="""+v_default_args+""") as dag:
\n"""


    ########################
    #    Task Generator    #
    ########################
    v_DAG_TSK_Generator = ""
    for key in DPLF_Config["TaskList"]:
        v_sql_file = v_sql_path + key["TaskSQL"]
        v_DAG_TSK_Generator += """ 
    """+key["TaskId"]+"""= python_operator.PythonOperator(task_id='"""+key["TaskId"]+"""',
                                    python_callable=DPLF_Library.DPLF_Exec_SQL,
                                    op_args=['"""+v_sql_file+"""'],
                                    provide_context="""+key["provide_context"]+""",
                                    trigger_rule=TriggerRule."""+key["TriggerRule"]+""")
                                    \n"""


    ################################
    #    Dependencies Generator    #
    ################################
    v_TSK_Dep = "    "
    for key in DPLF_Config["TaskList"]:
        if key["Following_Task"] != "":
            v_TSK_Dep += key["TaskId"]+" >> "+key["Following_Task"]+"\n    "



    ###################################
    #    Python file to DAG bucket    #
    ###################################
    g_gs_client = storage.Client()
    v_composer_folder = 'europe-west1-ls-dp-imp-v001-16acad17-bucket'
    v_composer_bucket = g_gs_client.get_bucket(v_composer_folder)
    dag_blob = 'dags/DPL_LS_ETL.py'
    dag_blob = v_composer_bucket.blob(dag_blob)  
    v_text_file=(v_libraries+
                v_start_args +
                v_TaskDag +
                v_DAG_TSK_Generator +
                v_TSK_Dep
                )
    dag_blob.upload_from_string(v_text_file)



with models.DAG(
        'DynamicDAG',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    DPFL_DynamicDAG = python_operator.PythonOperator(task_id='DPL_LS_DynamicDAG',
                                                     python_callable=DPFL_DynamicDAG,
                                                     provide_context=True)