from datetime import datetime
import calendar
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import os, sys
sys.path.append(os.path.dirname(__file__))
sys.path.append(os.getcwd())
import common_functions as cf
#import abc_util as abc

# dag = DAG('GL_BALANCE', description='GL_BALANCE DEMO DAG',
#           schedule_interval=None,
#           start_date=datetime(2017, 3, 20), catchup=False)

with DAG(dag_id="f_gl_balance", start_date=datetime(2017, 3, 20), catchup=False, tags=["example"], description='GL_BALANCE DEMO DAG') as dag:

    balance_company = Variable.get('balance_company', default_var=None)
    balance_query = Variable.get('balance_query', default_var=None)
    fiscal_year_query = Variable.get('gl_balance_fiscal_year', default_var=None)
    transform_query = Variable.get('transform_query', default_var=None)
    v_month_target_query = Variable.get('v_month_target_query', default_var=None)
    current_month_stage_query = Variable.get('current_month_stage_query', default_var=None)
    target_delete_query = Variable.get('target_delete_query', default_var=None)
    rejected_records_query = Variable.get('rejected_records_query', default_var=None)
    bad_data_table_name = Variable.get('bad_data_table_name', default_var=None)

    FEED_ID = 'GL_001'


    def get_abc_data():

        # args = abc.get_parameter_data(FEED_ID)
        args = {'stage_table': 'STG_GL_BALANCE_AIRFLOW', 'target_table': 'F_GL_BALANCE_AIRFLOW'}
        return args


    def truncate_from_stage(**kwargs):

        ti = kwargs['ti']
        args = ti.xcom_pull(task_ids='get_abc_data')

        stage_table_name = args['stage_table']
        cf.delete_data('oracle_stage', 'STG_GL_BALANCE_AIRFLOW')
        # cf.delete_data('oracle_stage', stage_table_name)
        return


    def generate_query(month_of_year):

        #Logic for generating LCUR_OPNNG_BAL_AMT column
        opening_balance_account = "a.db_beg_bal + a.cr_beg_bal "
        x=0
        while x < month_of_year:
            if x==0:
                opening_balance_account_pre = "ignore"
            elif x<=9:
                opening_balance_account_pre = "+ a.db_amount_0{} + a.cr_amount_0{}".format(x,x)
            else:
                opening_balance_account_pre = "+ a.db_amount_{} + a.cr_amount_{}".format(x,x)
            
            if opening_balance_account_pre == "ignore":
                print("skipping")
            else:
                opening_balance_account = opening_balance_account+opening_balance_account_pre
            x=x+1
            
        #Logic for generating LCUR_CLSNG_BAL_AMT column
        closing_balance_account = "a.db_beg_bal + a.cr_beg_bal"
        x=1
        while x <= month_of_year:
            closing_balance_account_pre = "+ a.db_amount_0{} + a.cr_amount_0{}".format(x,x) if x<=9 else "+ a.db_amount_{} + a.cr_amount_{}".format(x,x)
            closing_balance_account = closing_balance_account+closing_balance_account_pre
            x=x+1
            
        #Logic for generating LCUR_YTD_DR_AMT column
        x=1
        ytd_dr_account = ""
        while x <= month_of_year:
            ytd_dr_account_pre = "a.db_amount_0{}".format(x) if x<=9 else "a.db_amount_{}".format(x)
            if ytd_dr_account == "":
                ytd_dr_account = ytd_dr_account_pre
            else:
                ytd_dr_account = ytd_dr_account+" + "+ytd_dr_account_pre
            x=x+1
            
        #Logic for generating LCUR_YTD_CR_AMT column
        x=1
        ytd_cr_account = ""
        while x <= month_of_year:
            ytd_cr_account_pre = "a.cr_amount_0{}".format(x)
            if x == "1":
                ytd_cr_account = "a.cr_amount_01"
            elif x == "2":
                ytd_cr_account = "a.cr_amount_02 + a.cr_amount_02"
            else:
                ytd_cr_account = ytd_cr_account+" + "+ytd_cr_account_pre
            x=x+1
            
        query = balance_query.format(month_of_year,opening_balance_account,month_of_year,month_of_year,\
                                    closing_balance_account,ytd_dr_account,ytd_cr_account,str(month_of_year).zfill(2),"1,2",fiscal_year_query)\
                                    .replace('_amount_010','_amount_10')\
                                    .replace('_amount_011','_amount_11')\
                                    .replace('_amount_012','_amount_12')
        
        return query


    def load_from_source_to_stage(month_of_year, **kwargs):

        ti = kwargs['ti']
        args = ti.xcom_pull(task_ids='get_abc_data')

        stage_table_name = args['stage_table']

        query = generate_query(month_of_year)
        source_data = cf.read_data('oracle_source', query, num_rows=1000)
        cf.insert_data('oracle_stage', stage_table_name, source_data)       


    def delete_from_target(**kwargs):

        ti = kwargs['ti']
        args = ti.xcom_pull(task_ids='get_abc_data')
        print('current_month_stage_query-->', current_month_stage_query)
        current_month_stage_data = cf.read_data('oracle_stage', current_month_stage_query)
        print('current_month_stage_data-->', current_month_stage_data)
        current_month_stage_list = []
        for a in current_month_stage_data:
            current_month_stage_list.append(str(a).replace('(','').replace(')','').replace(',','').replace('\'',''))
        v_month_target_query1 = v_month_target_query.format(current_month_stage_list)
        v_month_target_query2 = str(v_month_target_query1).replace('[','(').replace(']',')')
        v_month_target_data = cf.read_data('oracle_target', v_month_target_query2)
        # cf.insert_data('oracle_stage', stage_table_name, source_data)
        v_month_target_data_list = []
        for a in v_month_target_data_list:
            v_month_target_data_list.append(str(a).replace('(','').replace(')','').replace(',','').replace('\'',''))
        target_delete_query1 = target_delete_query.format(v_month_target_data_list)
        target_delete_query2 = str(target_delete_query1).replace('[','(').replace(']',')')
        print('v_month_target_data_list-->', v_month_target_data_list)
        print('target_delete_query2-->', target_delete_query2)
        if v_month_target_data_list != []:
            cf.delete_data('oracle_target', query=target_delete_query2)
        print(v_month_target_data)
        

    def load_from_stage_to_target(**kwargs):

        ti = kwargs['ti']
        args = ti.xcom_pull(task_ids='get_abc_data')

        target_table_name = args['target_table']
        
        #print('transform_query-->',transform_query)
        target_data_from_stage = cf.read_data('oracle_stage', transform_query)
        cf.insert_data('oracle_target', target_table_name, target_data_from_stage)
        return

    
    def load_bad_data_to_target(**kwargs):

        ti = kwargs['ti']
        args = ti.xcom_pull(task_ids='get_abc_data')

        # target_table_name = args['target_table']
        
        #print('transform_query-->',transform_query)
        bad_data_from_stage = cf.read_data('oracle_stage', rejected_records_query)
        cf.insert_data('oracle_target', bad_data_table_name, bad_data_from_stage)
        return

    # task definitions
    wait_for_gl_dimensions = DummyOperator(task_id='wait_for_gl_dimensions', dag=dag)
    
    get_abc_data_task = PythonOperator(task_id='get_abc_data',
                                python_callable=get_abc_data,
                                dag=dag)

    stage_loading_completed = DummyOperator(task_id='stage_loading_completed', dag=dag)

    final_notification = DummyOperator(task_id='final_notification', dag=dag)

    truncate_from_stage = PythonOperator(task_id='truncate_from_stage', 
                                python_callable=truncate_from_stage,
                                provide_context=True,
                                dag=dag)
    
    wait_for_gl_dimensions.set_downstream(get_abc_data_task)
    get_abc_data_task.set_downstream(truncate_from_stage)

    def dynamic_extract_operator(task_id, pc_params, month_of_year):
        return PythonOperator(task_id=task_id, 
                            python_callable=load_from_source_to_stage,
                            op_kwargs={"month_of_year": month_of_year},
                            provide_context=True,
                            dag=dag)
    
    delete_from_target = PythonOperator(task_id='delete_from_target', 
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

    with TaskGroup("ingest_from_source_to_stage") as group1:
        for i in range(0, 12):
            monthinteger = i + 1
            # month = datetime.date(1900, monthinteger, 1).strftime('%B')
            month = calendar.month_abbr[monthinteger]
            created_task = dynamic_extract_operator(task_id=f'extract_and_load_{month.lower()}_data',
                                                    pc_params=month.lower(), month_of_year=monthinteger)
            
    
    truncate_from_stage.set_downstream(group1)
    group1.set_downstream(stage_loading_completed)
    stage_loading_completed.set_downstream(delete_from_target)
    delete_from_target.set_downstream([load_from_stage_to_target_task,load_bad_data_to_target_task])
    final_notification.set_upstream([load_from_stage_to_target_task,load_bad_data_to_target_task])
