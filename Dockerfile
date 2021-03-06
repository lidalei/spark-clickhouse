FROM bde2020/spark-base:2.4.4-hadoop2.7
LABEL maintainer="Dalei <dalei.li@icloud.com>"

RUN apk update && apk add --no-cache build-base
RUN apk add python3-dev~=3.7.5 && pip3 install -U pip

ENV PYSPARK_PYTHON /usr/bin/python3

# https://vsupalov.com/cache-docker-build-dependencies-without-volume-mounting/
ADD ./requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

# Copy the source code
WORKDIR /app
COPY . /app

# clickhouse is the clickhouse service name in docker compose
CMD ["/bin/bash", "-c", "python3 store_items_metadata.py --clickhouse-server=${CLICKHOUSE_SERVER} --clickhouse-db=${CLICKHOUSE_DB} --clickhouse-user=${CLICKHOUSE_USER} --clickhouse-password=${CLICKHOUSE_PASSWORD} --items-uri=${ITEMS_URI} --items-file=${ITEMS_FILE} --metadata-uri=${METADATA_URI} --metadata-file=${METADATA_FILE}"]