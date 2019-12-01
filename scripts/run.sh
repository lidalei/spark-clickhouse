export AIRFLOW_HOME=~/airflow
pip3 install -r scripts/requirements.txt

airflow initdb
airflow webserver -p 8080 &
airflow scheduler &

airflow trigger_dag -sd scripts -l import-amazon-reviews