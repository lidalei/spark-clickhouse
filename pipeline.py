from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Dalei',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 1),
    'email': ['dalei.li@icloud.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('import-amazon-reviews', default_args=default_args, schedule_interval=timedelta(hours=8))

t1 = BashOperator(
    task_id='build_images',
    bash_command='docker-compose build',
    dag=dag
)

t2 = BashOperator(
    task_id='create_service',
    bash_command='docker-compose up --no-start',
    dag=dag
)

t3 = BashOperator(
    task_id='start_clickhouse',
    bash_command='docker-compose start clickhouse',
    dag=dag
)

t4 = BashOperator(
    task_id='start_spark_job',
    bash_command='docker-compose start spark-job',
    dag=dag
)

t4.set_upstream(t3)
t3.set_upstream(t2)
t2.set_upstream(t1)
