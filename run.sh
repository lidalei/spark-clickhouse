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

# build docker containers
docker-compose build
# run spark job to read those files and write to clickhouse
export CLICKHOUSE_PASSWORD=B1t7XFtEPGDUEIKD
docker-compose up   
