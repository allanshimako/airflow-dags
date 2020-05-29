#!/usr/bin/env python
# coding: utf-8

# In[2]:


from datetime import timedelta
import papermill as pm
import os

from airflow import DAG

# Operators are used to create the tasks
from airflow.operators.bash_operator import BashOperator
from airflow.operators.papermill_operator import PapermillOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


# In[3]:


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'allan',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['allanshimako@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_spark',
    default_args=default_args,
    description='test',
    schedule_interval="@once",
)


dag.doc_md = __doc__

# t1 will only send start_date from bash
t1 = BashOperator(
    task_id='user',
    bash_command='whoami',
    dag=dag,
    xcom_push=True, 
)

t2 = BashOperator(
    task_id='echo_user',
    bash_command='echo {{ task_instance.xcom_pull(task_ids="user")}}',
    dag=dag,
    xcom_push=True, 
)

def papermill_run (config):
    """
    run the notebooks called in Python operators
    """
    pm.execute_notebook(
    config.get('input_nb', None),
    config.get('output_nb', None),
    parameters=config.get('parameters', None))

t3 = PythonOperator(
        task_id = 'papermill_test',
        python_callable=papermill_run,
        op_kwargs={'config': {
            'input_nb': '/opt/dags/spark_test.ipynb',
            'output_nb': '/opt/dags/spark_test_{{ ds }}.ipynb',
            'parameters': {'file':'/opt/dags/test'
                           , 'pyspark_python':'python3',
                          }
        }
                  },
        dag=dag,
)
 
    
## PapermillOperator is not yet operational, there is an error when submitting it. 
## It was corrected and pushed to master but not yet released    
"""t2 = PapermillOperator(
    task_id ='test_papermill',
    input_nb='/mnt/d/notebooks/test.ipynb',
    output_nb='/mnt/d/notebooks/test_out.ipynb',
    parameters={'file':'/mnt/d/notebooks/test'},
    dag=dag
)"""


# 

t1 >> t2 >>t3


# In[ ]:




