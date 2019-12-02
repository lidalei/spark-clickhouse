#!/bin/bash

## run spark job to download josn files and write to clickhouse	
# export ENV variables to overwrite .env	
# export CLICKHOUSE_PASSWORD=B1t7XFtEPGDUEIKD	
# export ITEMS_DIR=data/items	
# export METADATA_DIR=data/metadata	
# verify configuration	
docker-compose config	
# build docker containers	
docker-compose build	
# create service	
docker-compose up --no-start	
# start clickhouse	
docker-compose start clickhouse	
# start spark-job	
docker-compose start spark-job	

# Stops containers and removes containers, networks, volumes, and images created by up.	
# docker-compose down	

# check runnig containers status	
# docker-compose ps	
# docker-compose logs SERVICE

## WIP, airflow pipeline
# export AIRFLOW_HOME=~/airflow
# pip3 install -r scripts/requirements.txt

# airflow initdb
# airflow webserver -p 8080 &
# airflow scheduler &

# airflow trigger_dag -sd scripts -l import-amazon-reviews