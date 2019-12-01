pip3 install -r scripts/requirements.txt

airflow initdb &
airflow webserver -p 8080 &
airflow scheduler &

python3 scripts/pipeline.py