echo "download files"
mkdir -p data/items
FILE=data/items/item_dedup.json.gz
if [ -f "$FILE" ]; then
    echo "$FILE exist"
else
    echo "download item_dedup file"
    curl -o $FILE https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/item_dedup.json.gz
fi

mkdir -p data/metadata
FILE=data/metadata/metadata.json.gz
if [ -f "$FILE" ]; then
    echo "$FILE exist"
else
    echo "download metadata file"
    curl -o data/metadata/metadata.json.gz https://s3-eu-west-1.amazonaws.com/bigdata-team/job-interview/metadata.json.gz
fi
echo "files are ready"

## docker postinstall
# https://docs.docker.com/install/linux/linux-postinstall/
# sudo groupadd docker
# sudo usermod -aG docker $USER
# exit

## reconnect o VM
# gcloud compute --project "youtube8m-winner" ssh --zone "europe-west4-a" "instance-1"
# activate the changes to groups
# newgrp docker
# verify
# docker run hello-world
## end docker postinstall

## run spark job to read those files and write to clickhouse
# export to overwrite .env
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