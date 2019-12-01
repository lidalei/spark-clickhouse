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

# instead of relying on .env, we can set environment variables here
t1 = BashOperator(
    task_id='build_images',
    bash_command='docker-compose build',
    env={
        'ITEMS_URI': 'https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/item_dedup.json.gz',
        'ITEMS_FILE': 'data/items/item_dedup.json',
        'METADATA_URI': 'https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/metadata.json.gz',
        'METADATA_FILE': 'data/metadata/metadata.json',
    },
    dag=dag
)

t2 = BashOperator(
    task_id='create_service',
    bash_command='docker-compose up --no-start',
    env={},
    dag=dag
)

t3 = BashOperator(
    task_id='start_clickhouse',
    bash_command='docker-compose start clickhouse',
    env={},
    dag=dag
)

# t4 = BashOperator(
#     task_id='start_spark_job',
#     bash_command='docker-compose start spark-job',
#     env={},
#     dag=dag
# )
#
# t4.set_upstream(t3)
t3.set_upstream(t2)
t2.set_upstream(t1)
