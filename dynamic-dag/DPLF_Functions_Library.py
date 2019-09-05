from __future__ import print_function
import json
from airflow import AirflowException
from airflow.contrib.hooks.bigquery_hook import BigQueryHook

# Global Variables for library
g_source_composer_bucket_path = '/home/airflow/gcs/dags/'
g_json_composer_bucket_path = g_source_composer_bucket_path+'json/'
g_sql_composer_bucket_path = g_source_composer_bucket_path+'sql/'
g_conn_id = 'bigquery_default'
g_bq_hook = BigQueryHook(bigquery_conn_id=g_conn_id)
g_config_file = "/home/airflow/gcs/dags/json/DPLF_DAG_Generator_Config.json"

# Access configuration
def DPLF_Access_ReadConfig(p_config_file="/home/airflow/gcs/dags/json/DPLF_DAG_Generator_Config.json"):
    v_json_data = open(p_config_file).read()
    v_config = json.loads(v_json_data)
    return v_config


# Extractor of files and tasks from Json
def DPLF_GetValueByKey(p_list, p_key):
    if p_key in p_list:
        return p_list[p_key]
    else:
        return ""


DPLF_Config = DPLF_Access_ReadConfig(g_config_file)


# Parsing function that substitutes sql variables placeholders from Json 
def DPLF_ParseSQLCommand(p_sqlcommand):
    v_command = p_sqlcommand
    for item in (DPLF_Config["SQLVariables"]):
        if item["id"] in v_command:
            v_SQL_P = v_command.replace(item["id"], item["value"])
            v_command = v_SQL_P
    return v_command


# Executing SQL function 
def DPLF_Exec_SQL(p_sql_file, **kwargs):
    try:
        p_file_sql = p_sql_file
        fd = open(p_file_sql, 'r')
        sqlFile = fd.read()
        fd.close()
        v_sqlCommands = sqlFile.split(';')
        for v_commands in v_sqlCommands:
            if len(v_commands.rstrip())==0:
                return 0
            else:
                v_command_parsed = DPLF_ParseSQLCommand(v_commands)
                query = "#standardSQL" + "\n" + v_command_parsed
                v_conn = g_bq_hook.get_conn()
                v_curs = v_conn.cursor()
                v_curs.run_query(query, use_legacy_sql=False)
        return 0
    except:
        raise AirflowException('ERROR - Incorrect SQL Query')


