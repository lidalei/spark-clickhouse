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

# build docker containers
sudo docker-compose build
# run spark job to read those files and write to clickhouse
# export CLICKHOUSE_PASSWORD=B1t7XFtEPGDUEIKD
# export ITEMS_DIR=data/items
# export METADATA_DIR=data/metadata
sudo docker-compose up

# check runnig containers status
# docker stats

# forward clickhouse http interface
gcloud compute --project "youtube8m-winner" ssh --zone "europe-west4-a" "instance-3" -- -L 4040:localhost:4040 -L 8888:localhost:8888

# give 127.0.0.1:8888 a public host so that grafana in a container does not resolve it wronlgy
ngrok http 8888

# start grafana
docker run -d --name=grafana -p 3000:3000 \
    -e GF_INSTALL_PLUGINS=vertamedia-clickhouse-datasource \
    -e GF_SECURITY_ADMIN_USER=guess \
    -e GF_SECURITY_ADMIN_PASSWORD=youneverknow \
    grafana/grafana

