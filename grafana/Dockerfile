FROM grafana/grafana-oss:13.0.2

# Fly volumes are owned by root.
USER root

# Plugins live outside /var/lib/grafana, which is the volume.
ENV GF_PATHS_PLUGINS=/opt/grafana-plugins
RUN grafana cli --pluginsDir /opt/grafana-plugins plugins install yesoreyeram-infinity-datasource 4.1.1

COPY provisioning /etc/grafana/provisioning
