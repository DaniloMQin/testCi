# -*- coding: utf-8 -*-

# LS_POC_MasterDAG 1.08 Version

# Changelog:
# 1.08 Version - 03/09/2019 - M.Cibin - New JSON format support
# 1.07 Version - 02/09/2019 - A.Serena - Consistency check on json and csv fields 
# 1.06 Version - 02/09/2019 - M.Cibin - Error Table improvement
# 1.05 Version - 01/09/2019 - A.Serena - LOG function fix
# 1.04 Version - 29/08/2019 - M.Cibin - SQL functions creation
# 1.03 Version - 12/06/2019 - A.Serena - Staging tables management
# 1.02 Version - 12/06/2019 - A.Serena - Metadata management
# 1.01 Version - 06/06/2019 - A.Serena - correct maangement of BigQuery_Insert_Into_IngestionTable function 
# 1.00 Version - 30/05/2019 - A.Serena - Main DAG management

import datetime
import logging
import subprocess
import time
import os
from airflow import configuration
from airflow import models
from airflow import AirflowException
from airflow.contrib.hooks import gcs_hook
from airflow.contrib.operators import dataflow_operator
from airflow.operators import python_operator
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import storage
from google.cloud import bigquery
from airflow import AirflowException
import json
import google.cloud.logging
import logging
from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging


# We are going to define all the global variables

def DPLF_ReadVariables(p_config_file):
    json_data = open(p_config_file).read()
    var = json.loads(json_data)
    g_var_dic = var["GlobalVariables"]
    return g_var_dic


Vars = DPLF_ReadVariables('/home/airflow/gcs/dags/json/LS_JSON_Variables.json')

g_project = Vars["g_project"]
g_composer_bucket_path = Vars["g_composer_bucket_path"]
g_input_bucket = Vars["g_input_bucket"]
g_output_bucket = Vars["g_output_bucket"]
g_failed_bucket = Vars["g_failed_bucket"]
g_temp_folder = Vars["g_temp_folder"]
g_DLPF_SetDebug = Vars["p_DLPF_SetDebug"]
g_delimiter = Vars["p_CSV_field_delimiter"]
g_encoding = Vars["p_CSV_encoding"]
g_gs_client = storage.Client()
g_bq_client = bigquery.Client()
g_log_client = google.cloud.logging.Client(project=g_project)


now = datetime.datetime.now()
g_tstamp = now.strftime("%Y%m%d%H%M%S")


def DPLF_SetLoggingHandler(p_logging_prefix=Vars["p_logging_prefix"]):
    global g_log_client
    handler = g_log_client.logger(p_logging_prefix)
    return handler


def DPLF_WriteLog(p_type, p_message, p_timestamp="", p_tablename="", p_sourceID="", p_jobID="", p_taskID=""):
    logger = DPLF_SetLoggingHandler()
    v_dp_action = p_message[:p_message.find('-')]
    v_message = p_message.replace(p_message[:p_message.find('-')+1], '')
    label_payload = ({
                    'load_ts': p_timestamp,
                    'dp_action': v_dp_action,
                    'source_id': p_sourceID,
                    'table_name': p_tablename,
                    'job_id': str(p_jobID),
                    'task_id': str(p_taskID)
                    })
    v_message = p_timestamp + '-' + p_message
    if p_type == 'INFO':
        logger.log_text(
         v_message,
         severity=p_type,
         labels=label_payload
        )
        print('Info: {}'.format(v_message))
    elif p_type == 'WARNING':
        logger.log_text(
         v_message,
         severity=p_type,
         labels=label_payload
        )
        print('Warning: {}'.format(v_message))
    elif p_type == 'ERROR':
        logger.log_text(
         v_message,
         severity=p_type,
         labels=label_payload
        )
        print('Error: {}'.format(v_message))
    elif p_type == 'DEBUG':
        if g_DLPF_SetDebug == 1:
            logger.log_text(
             v_message,
             severity=p_type,
             labels=label_payload
            )
            print('Debug: {}'.format(v_message))


def DPLF_ReadConfig(p_config_file='/home/airflow/gcs/dags/json/DPL_Schema_ABF_PROJ_NPO.json'):
    json_data = open(p_config_file).read()
    DPLF_Config = json.loads(json_data)
    p_fields_dict = DPLF_Config["fields"]
    p_fields_string = ""
    p_fields_list = []
    p_type_list = []
    dupes = 0
    for key in DPLF_Config["fields"]:
        p_fields_string += (key["column_name"])+','
        p_fields_list.append(key["column_name"])
        p_type_list.append(key["type"])
        if "primary_key" in key:
            if key["primary_key"] == "y":
                dupes = dupes + 1
    p_fields_string = p_fields_string[:-1]
    return p_fields_dict, p_fields_string, p_fields_list, p_type_list, dupes


g_fields_dict, g_fields, g_l_fields, g_types, g_pr_keys = DPLF_ReadConfig(p_config_file=g_composer_bucket_path+'json/DPL_Schema_ABF_PROJ_NPO.json')

g_ft_list = []
for i in range(len(g_l_fields)):
    g_ft_list.append([g_l_fields[i], g_types[i]])


DATAFLOW_FILE = os.path.join(configuration.get('core', 'dags_folder'), 'dataflow', 'DPL_LS_DF_Pipeline_01.py')

DEFAULT_DAG_ARGS = {
    'start_date': datetime.datetime(2000, 1, 31),
    'retries': 0,
    'project_id': g_project, 
    'dataflow_default_options': {
        'project': g_project,
        'temp_location': g_temp_folder,
        'runner': 'DataflowRunner'
    }
}


def DPLF_ConsistencyCheck(**kwargs):
    v_st = kwargs['dag_run'].conf['name']
    v_name_no_ext = v_st[:v_st.rfind(".")]
    v_split = v_name_no_ext.split("_")
    v_tab = "_".join(v_split[2:])
    v_filename = kwargs['dag_run'].conf['name']  
    v_bucket = g_gs_client.get_bucket(g_input_bucket)
    blob = storage.Blob(v_filename, v_bucket)
    v_content = blob.download_as_string()
    v_decod = v_content.decode(g_encoding)  # bytes to str 
    lines = v_decod.split('\r')[:1]
    v_csv_fields = 0
    for r in lines:
        vfileds = str(r)
        for n in vfileds.split(g_delimiter):
            v_csv_fields += 1
    v_fields, v_l_fields = DPLF_ReadConfig(p_config_file=g_composer_bucket_path+'json/DPL_Schema_ABF_PROJ_NPO.json')
    v_json_fields = len(v_l_fields)
    if v_json_fields > v_csv_fields:
        DPLF_WriteLog('ERROR',
                      'INGESTION-number of fields in json schema is higher than in csv schema',
                      p_tablename=v_tab,
                      p_timestamp=g_tstamp,
                      p_sourceID=v_filename,
                      p_jobID=kwargs.get('dag', None),
                      p_taskID=kwargs.get('task',  None)
                      )
        raise AirflowException('number of fields in json schema is higher than in csv schema')
    elif v_json_fields < v_csv_fields:
        DPLF_WriteLog('WARNING',
                      'INGESTION-number of fields in json schema is higher than in csv schema; keep proceeding with ingestion phase',
                      p_tablename=v_tab,
                      p_timestamp=g_tstamp,
                      p_sourceID=v_filename,
                      p_jobID=kwargs.get('dag', None),
                      p_taskID=kwargs.get('task',  None)
                      )
    elif v_json_fields == v_csv_fields:
        DPLF_WriteLog('INFO',
                      'INGESTION-Consistency check: OK',
                      p_tablename=v_tab,
                      p_timestamp=g_tstamp,
                      p_sourceID=v_filename,
                      p_jobID=kwargs.get('dag', None),
                      p_taskID=kwargs.get('task',  None)
                      )


def DPLF_move_into_arc_bucket(target_bucket, **kwargs):
    v_st = kwargs['dag_run'].conf['name']
    v_name_no_ext = v_st[:v_st.rfind(".")]
    v_split = v_name_no_ext.split("_")
    v_table_name = "_".join(v_split[2:])
    v_sourceID = kwargs['dag_run'].conf['name']
    # Establish a connection hook to GoogleCloudStorage.
    conn = gcs_hook.GoogleCloudStorageHook()
    # Arguments are passed by the Cloud Function
    source_bucket = kwargs['dag_run'].conf['bucket']
    source_object = kwargs['dag_run'].conf['name']
    target_object = source_object[:source_object.rfind('.')]+"_"+str(g_tstamp)+source_object[source_object.rfind('.'):]
    target_bucket = g_output_bucket
    # Upon completion of the previous task, the .csv file is moved to the completion bucket 
    DPLF_WriteLog('INFO', 
                  "INGESTION-Copying " + source_object + " from " + source_bucket + " into " + target_bucket,
                  p_tablename=v_table_name,
                  p_timestamp=g_tstamp,
                  p_sourceID=v_sourceID,
                  p_jobID=kwargs.get('dag', None),
                  p_taskID=kwargs.get('task',  None)
                  )
    conn.copy(source_bucket, source_object, target_bucket, target_object)
    DPLF_WriteLog('INFO', 
                  "INGESTION-Deleting " + source_object + " from " + source_bucket,
                  p_tablename=v_table_name,
                  p_timestamp=g_tstamp,
                  p_sourceID=v_sourceID,
                  p_jobID=kwargs.get('dag', None),
                  p_taskID=kwargs.get('task',  None)
                  )
    conn.delete(source_bucket, source_object)


def DPLF_move_into_inv_bucket(target_bucket, **kwargs):
    v_st = kwargs['dag_run'].conf['name']
    v_name_no_ext = v_st[:v_st.rfind(".")]
    v_split = v_name_no_ext.split("_")
    v_table_name = "_".join(v_split[2:])
    v_sourceID = kwargs['dag_run'].conf['name']
    # Establish a connection hook to GoogleCloudStorage.
    conn = gcs_hook.GoogleCloudStorageHook()
    # Arguments are passed by the Cloud Function
    source_bucket = kwargs['dag_run'].conf['bucket']
    source_object = kwargs['dag_run'].conf['name']
    target_object = source_object
    target_bucket = g_failed_bucket
    # Upon failure of the previous task, the .csv file is moved to the invalid bucket 
    DPLF_WriteLog('INFO', 
                  "INGESTION-Copying " + source_object + " from " + source_bucket + " into " + target_bucket,
                  p_tablename=v_table_name,
                  p_timestamp=g_tstamp,
                  p_sourceID=v_sourceID,
                  p_jobID=kwargs.get('dag', None),
                  p_taskID=kwargs.get('task',  None)
                  )
    conn.copy(source_bucket, source_object, target_bucket, target_object)
    DPLF_WriteLog('INFO', 
                  "INGESTION-Deleting " + source_object + " from " + source_bucket,
                  p_tablename=v_table_name,
                  p_timestamp=g_tstamp,
                  p_sourceID=v_sourceID,
                  p_jobID=kwargs.get('dag', None),
                  p_taskID=kwargs.get('task',  None)
                  )
    conn.delete(source_bucket, source_object)


def DPLF_ExecOSCmd(p_oscmd, **kwargs):
    try:
        p = subprocess.Popen(p_oscmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE )
        res = p.communicate()
        if p.returncode != 0:
            AirflowException("ERROR")
        return p.returncode
    except Exception as e:
        AirflowException("ERROR")
        return 1


def DPLF_Check_Conversion(p_json_dict_list):
    """
    The output of this function will be coded conversion operations, with the following format:
    ['DATE', input_format, output_format]:
        run date_converter(x, input_format, output_format) on x
    ['NUMERIC', decimal_delimiter, thousand_delimiter]:
        run numeric_converter(x, decimal_delimiter, thousand_delimiter) on x
    ['None']:
        leave the value alone.
    :param p_json_dict_list: Input taken from a json file containing the schema of a table.
    :return: List containing coded conversion operations that will be run on each row of the table.
    """
    operations = {}
    for json_dict in p_json_dict_list:
        field_type = json_dict["type"]
        field_name = json_dict["column_name"]
        if field_type == "NUMERIC":
            decimal_delimiter = "."
            thousand_delimiter = "None"
            if "separator_decimal" in json_dict:
                decimal_delimiter = json_dict["separator_decimal"]
            if "separator_thousand" in json_dict:
                thousand_delimiter = json_dict["separator_thousand"]
            operations[field_name]=["NUMERIC", decimal_delimiter, thousand_delimiter]
        elif field_type == "DATE":
            output_format = "YYYY-MM-DD"
            if "format" in json_dict:
                input_format = json_dict["format"]
            else:
                input_format = output_format
            operations[field_name]=["DATE", input_format, output_format]
        else:
            operations[field_name]=["None"]
    return operations


g_operations_dict = json.dumps(DPLF_Check_Conversion(g_fields_dict))


# Schedule_interval is set to None as the DAG is triggered by a Cloud Function 
with models.DAG(dag_id='LS_MasterDAG_01',
                description='A DAG triggered by an external Cloud Function',
                schedule_interval=None, concurrency=30, default_args=DEFAULT_DAG_ARGS) as dag:
    # Args required for the Dataflow job.
    job_args = {
        'input': 'gs://{{ dag_run.conf["bucket"] }}/{{ dag_run.conf["name"]}}', 
        'output_raw': 'RAW_{{dag_run.conf["name"][:dag_run.conf["name"].rfind("/")]}}.{{"_".join(dag_run.conf["name"][:dag_run.conf["name"].rfind(".")].split("_")[2:])}}',  # takes out the file name removing ".csv" from it
        'output_err': 'PRZ_{{dag_run.conf["name"][:dag_run.conf["name"].rfind("/")]}}.{{"_".join(dag_run.conf["name"][:dag_run.conf["name"].rfind(".")].split("_")[2:])_ERR}}',
        'output_prz': 'PRZ_{{dag_run.conf["name"][:dag_run.conf["name"].rfind("/")]}}.{{"_".join(dag_run.conf["name"][:dag_run.conf["name"].rfind(".")].split("_")[2:])}}',
        'fields': g_fields, 
        'load_dt': '{{ dag_run.conf["bqTimestamp"]}}',
        'op_dict': g_operations_dict
    }

    # Main Dataflow task
    TSK_dataflow_file_ingestion = dataflow_operator.DataFlowPythonOperator(
        task_id="tsk-dataflow-file-ingestion",
        py_file=DATAFLOW_FILE,
        options=job_args)

    # Upon Dataflow task success the TSK_move_into_arc_bucket starts 
    TSK_move_into_arc_bucket = python_operator.PythonOperator(task_id='TSK_move_into_arc_bucket',
                                                              python_callable=DPLF_move_into_arc_bucket,
                                                              op_args=[g_output_bucket],
                                                              provide_context=True,
                                                              trigger_rule=TriggerRule.ALL_SUCCESS)

    # Upon Dataflow task failure the TSK_move_into_inv_bucket starts 
    TSK_move_into_inv_bucket = python_operator.PythonOperator(task_id='TSK_move_into_inv_bucket',
                                                              python_callable=DPLF_move_into_inv_bucket,
                                                              op_args=[g_failed_bucket],
                                                              provide_context=True,
                                                              trigger_rule=TriggerRule.ONE_FAILED)

    DPLF_ConsistencyCheck = python_operator.PythonOperator(task_id='DPLF_ConsistencyCheck',
                                                           python_callable=DPLF_ConsistencyCheck,
                                                           provide_context=True)

    # Define execution order
    DPLF_ConsistencyCheck >> TSK_move_into_inv_bucket
    DPLF_ConsistencyCheck >> TSK_dataflow_file_ingestion >> [TSK_move_into_inv_bucket, TSK_move_into_arc_bucket]
