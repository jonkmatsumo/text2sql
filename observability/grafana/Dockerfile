FROM grafana/grafana:10.0.0

# Copy provisioning configurations
COPY provisioning /etc/grafana/provisioning

# Copy dashboards to a location outside /var/lib/grafana (which is a volume)
COPY dashboards /etc/grafana/dashboards
