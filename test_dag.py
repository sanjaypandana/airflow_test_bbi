from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.utils.task_group import TaskGroup

with DAG(
   "using_task_group",
   default_args={'owner': 'airflow'},
   start_date=days_ago(2),
   schedule_interval=None,
) as dag:
   start_task = BashOperator(
       task_id="start_task",
       bash_command="echo start",
   )

   end_task = BashOperator(
       task_id="end_task",
       bash_command="echo end",
   )

   with TaskGroup(group_id="tasks_1") as tg1:
       previous_echo = BashOperator(
           task_id="echo_tg1",
           bash_command="echo wow"
       )
       for i in range(10):
           next_echo = BashOperator(
               task_id=f"echo_tg1_{i}",
               bash_command="echo wow"
           )
           previous_echo >> next_echo
           previous_echo = next_echo

   with TaskGroup(group_id="tasks_2") as tg2:
       previous_echo = BashOperator(
           task_id="echo_tg2",
           bash_command="echo wow"
       )
       for i in range(10):
           next_echo = BashOperator(
               task_id=f"echo_tg2_{i}",
               bash_command="echo wow"
           )
           previous_echo >> next_echo
           previous_echo = next_echo

   start_task >> [tg1, tg2] >> end_task