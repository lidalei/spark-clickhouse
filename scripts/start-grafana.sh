# start grafana
docker run -d --name=grafana -p 3000:3000 \
    -e GF_INSTALL_PLUGINS=vertamedia-clickhouse-datasource \
    -e GF_SECURITY_ADMIN_USER=guess \
    -e GF_SECURITY_ADMIN_PASSWORD=youneverknow \
    grafana/grafana
