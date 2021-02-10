from datetime import datetime
import calendar
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
#import abc_util as abc
import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))
import common_functions as cf

# dag = DAG('GL_BALANCE', description='GL_BALANCE DEMO DAG',
#           schedule_interval=None,
#           start_date=datetime(2017, 3, 20), catchup=False)

with DAG(dag_id="f_gl_detail", start_date=datetime(2017, 3, 20), catchup=False, tags=["example"], description='GL_DETAIL DEMO DAG') as dag:

    gl_detail_run_decision_query = Variable.get('gl_detail_run_decision_query', default_var=None)
    gl_detail_source_query = Variable.get('gl_detail_source_query', default_var=None)
    gl_detail_update_status_query = Variable.get('gl_detail_update_status_query', default_var=None)
    gl_detail_kpi_fin_run_status_query = Variable.get('gl_detail_kpi_fin_run_status_query', default_var=None)
    gl_detail_target_good_records_query = Variable.get('gl_detail_target_good_records_query', default_var=None)
    gl_detail_rejected_records_query = Variable.get('gl_detail_rejected_records_query', default_var=None)
    #current_month_stage_query = Variable.get('current_month_stage_query', default_var=None)
    #target_delete_query = Variable.get('target_delete_query', default_var=None)
    #rejected_records_query = Variable.get('rejected_records_query', default_var=None)
    #bad_data_table_name = Variable.get('bad_data_table_name', default_var=None)

    FEED_ID = 'GL_002'

    def check_dependency():
          
        lawson_status = cf.read_data('oracle_source', gl_detail_run_decision_query)
        print('lawson_status-->', lawson_status)
        if str(lawson_status) != "[('P',)]":
            raise


    def get_abc_data():

        # args = abc.get_parameter_data(FEED_ID)
        args = {'stage_table': 'STG_GL_DETAIL_AIRFLOW', 'target_table': 'F_GL_DETAIL_AIRFLOW'}
        return args


    def truncate_from_stage(**kwargs):

        ti = kwargs['ti']
        args = ti.xcom_pull(task_ids='get_abc_data')

        stage_table_name = args['stage_table']
        cf.truncate_data('oracle_stage', 'STG_GL_DETAIL_AIRFLOW')
        # cf.delete_data('oracle_stage', stage_table_name)
        return


    def load_from_source_to_stage(**kwargs):

        ti = kwargs['ti']
        args = ti.xcom_pull(task_ids='get_abc_data')

        stage_table_name = args['stage_table']

        source_data = cf.read_data('oracle_source', gl_detail_source_query, num_rows=1000)
        cf.insert_data('oracle_stage', stage_table_name, source_data)       


    def delete_from_target(**kwargs):

        ti = kwargs['ti']
        args = ti.xcom_pull(task_ids='get_abc_data')
        a = cf.call_stored_proc('PR_GL_DETAIL_CLEANUP_3MS_PRIOR', 'oracle_target')
        print(a)
        

    def load_from_stage_to_target(**kwargs):

        ti = kwargs['ti']
        args = ti.xcom_pull(task_ids='get_abc_data')

        target_table_name = args['target_table']
        
        target_data_from_stage = cf.read_data('oracle_stage', gl_detail_target_good_records_query)
        print('target_data_from_stage-->', target_data_from_stage)
        if target_data_from_stage:
            cf.insert_data('oracle_target', target_table_name, target_data_from_stage)
        else:
            print('No data to load')
        return

    
    def load_bad_data_to_target(**kwargs):

        ti = kwargs['ti']
        args = ti.xcom_pull(task_ids='get_abc_data')
        
        #print('transform_query-->',transform_query)
        bad_data_from_stage = cf.read_data('oracle_stage', gl_detail_rejected_records_query)
        cf.insert_data('oracle_target', 'F_GL_DETAIL_ERR', bad_data_from_stage)
        #return
    
    def update_run_parameters(**kwargs):

        ti = kwargs['ti']
        args = ti.xcom_pull(task_ids='get_abc_data')
        cf.run_query('oracle_target', gl_detail_update_status_query)


    def update_kpi_fin_run_status(**kwargs):

        ti = kwargs['ti']
        args = ti.xcom_pull(task_ids='get_abc_data')
        cf.run_query('oracle_target', gl_detail_kpi_fin_run_status_query)


    # task definitions
    check_dependency_task = PythonOperator(task_id='check_dependency',
                                python_callable=check_dependency,
                                dag=dag)
    
    get_abc_data_task = PythonOperator(task_id='get_abc_data',
                                python_callable=get_abc_data,
                                dag=dag)

    truncate_from_stage_task = PythonOperator(task_id='truncate_from_stage', 
                                python_callable=truncate_from_stage,
                                provide_context=True,
                                dag=dag)
    
    load_from_source_to_stage_task = PythonOperator(task_id='load_from_source_to_stage', 
                                python_callable=load_from_source_to_stage,
                                provide_context=True,
                                dag=dag)
    
    delete_from_target_task = PythonOperator(task_id='delete_from_target', 
                                python_callable=delete_from_target,
                                provide_context=True,
                                dag=dag)

    load_from_stage_to_target_task = PythonOperator(task_id='load_from_stage_to_target', 
                                python_callable=load_from_stage_to_target,
                                provide_context=True,
                                dag=dag)

    load_bad_data_to_target_task = PythonOperator(task_id='load_bad_data_to_target', 
                                python_callable=load_bad_data_to_target,
                                provide_context=True,
                                dag=dag)

    update_run_parameters_task = PythonOperator(task_id='update_run_parameters', 
                                python_callable=update_run_parameters,
                                provide_context=True,
                                dag=dag)
    
    update_kpi_fin_run_status_task = PythonOperator(task_id='update_kpi_fin_run_status', 
                                python_callable=update_kpi_fin_run_status,
                                provide_context=True,
                                dag=dag)
    
    final_notification = DummyOperator(task_id='final_notification', dag=dag)

    
    check_dependency_task.set_downstream(get_abc_data_task)
    get_abc_data_task.set_downstream(truncate_from_stage_task)
    truncate_from_stage_task.set_downstream(load_from_source_to_stage_task)
    load_from_source_to_stage_task.set_downstream(delete_from_target_task)
    delete_from_target_task.set_downstream([load_from_stage_to_target_task,load_bad_data_to_target_task])
    update_run_parameters_task.set_upstream([load_from_stage_to_target_task,load_bad_data_to_target_task])
    update_run_parameters_task.set_downstream(update_kpi_fin_run_status_task)
    update_kpi_fin_run_status_task.set_downstream(final_notification)
