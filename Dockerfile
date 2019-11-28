FROM bde2020/spark-base:2.4.4-hadoop2.7
LABEL maintainer="Dalei <dalei.li@icloud.com>"

RUN apk add build-base && apk add python3-dev~=3.7.5

# https://vsupalov.com/cache-docker-build-dependencies-without-volume-mounting/
ADD ./requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

# Copy the source code
WORKDIR /app
COPY . /app

# clickhouse is the clickhouse service name in docker compose
CMD ["/bin/bash", "-c", "python3 /app/store_items_metadata.py --clickhouse-server=clickhouse --batch-size=10000"]