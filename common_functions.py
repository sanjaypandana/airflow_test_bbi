import os
import json
##import cx_Oracle
import json
##from pypsexec.client import Client
from datetime import datetime
import calendar
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
#
def get_creds(app_name):

    #with open('/root/airflow/creds.json') as f:    
        #data = json.load(f)
    data = Variable.get('creds_json', deserialize_json=True, default_var=None)
    return data.get(app_name)


def execute_commands(vault_path_or_creds, program_name, arguments):
    
    creds = get_creds(vault_path_or_creds)
    c = Client(creds["host"], username=creds["username"], password=creds["password"], encrypt=False)
    c.connect()
    try:
        c.create_service()
        stdout = c.run_executable(program_name, arguments=arguments)
    except Exception as e:
        print(e)
        raise
    finally:
        c.remove_service()
        c.disconnect()
    output = []
    output = stdout[0].decode("utf-8").split("\r\n")
    
    for line in output:
        print(line)
    
    return output


def check_conn(database):
    creds = get_creds(database)
    dsn_tns = cx_Oracle.makedsn(creds['host'], creds['port'],service_name=creds['service_name'])
    with cx_Oracle.connect(user=creds['user'], password=creds['password'], dsn=dsn_tns) as conn:
        ver = conn.version
    return ver


def read_data(database, query, num_rows=None):

    print("query --> ", query)

    creds = get_creds(database)
    dsn_tns = cx_Oracle.makedsn(creds['host'], creds['port'], service_name=creds['service_name'])

    with cx_Oracle.connect(user=creds['user'], password=creds['password'], dsn=dsn_tns) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            if num_rows:
                rows = cursor.fetchmany(num_rows)
            else:
                rows = cursor.fetchall()
    
    return rows


def run_query(database, query):

    print("query --> ", query)

    creds = get_creds(database)
    dsn_tns = cx_Oracle.makedsn(creds['host'], creds['port'], service_name=creds['service_name'])

    with cx_Oracle.connect(user=creds['user'], password=creds['password'], dsn=dsn_tns) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)


def insert_data(database, table_name, data):

    num_of_col = len(data[0])
    print('num_of_col-->',num_of_col)
    values = ", ".join([f":{col+1}" for col in range(num_of_col)])
    insert_query = f"INSERT INTO {table_name} VALUES ({values})"

    creds = get_creds(database)
    dsn_tns = cx_Oracle.makedsn(creds['host'], creds['port'], service_name=creds['service_name'])
    with cx_Oracle.connect(user=creds['user'], password=creds['password'], dsn=dsn_tns) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(insert_query, data, batcherrors=True)
            if cursor.getbatcherrors():
                for error in cursor.getbatcherrors():
                    print("Error", error.message, "at row offset", error.offset)
                conn.rollback()
                raise ValueError("Error inserting the data to the table!")
            else:
                conn.commit()
                
                
def delete_data(database, table_name=None, query=None):
    
    if table_name:
        delete_query = f"DELETE FROM {table_name}"
        print("executing-->", delete_query)
    else:
        delete_query = query

    creds = get_creds(database)
    dsn_tns = cx_Oracle.makedsn(creds['host'], creds['port'], service_name=creds['service_name'])
    with cx_Oracle.connect(user=creds['user'], password=creds['password'], dsn=dsn_tns) as conn:
        with conn.cursor() as cursor:
            cursor.execute(delete_query)
            conn.commit()

            
def truncate_data(database, table_name=None, query=None):
    
    if table_name:
        truncate_query = f"TRUNCATE TABLE {table_name}"
        print("executing-->", truncate_query)
    else:
        truncate_query = query

    creds = get_creds(database)
    dsn_tns = cx_Oracle.makedsn(creds['host'], creds['port'], service_name=creds['service_name'])
    with cx_Oracle.connect(user=creds['user'], password=creds['password'], dsn=dsn_tns) as conn:
        with conn.cursor() as cursor:
            cursor.execute(truncate_query)
            conn.commit()


def call_stored_proc(proc_name, database):

    creds = get_creds(database)
    dsn_tns = cx_Oracle.makedsn(creds['host'], creds['port'], service_name=creds['service_name'])
    with cx_Oracle.connect(user=creds['user'], password=creds['password'], dsn=dsn_tns) as conn:
        with conn.cursor() as cursor:
            a = cursor.callproc(proc_name)
            print(a)
            conn.commit()
    return a
                
# print(check_conn("oracle_stage"))
# data = read_data(database="oracle_source", query="select * from glamounts", num_rows=10)
# print(data)
# print(type(data))
# print(len(data))
# print(data[0])
