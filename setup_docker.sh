#!/bin/bash
echo "Setting up SQL Monitor Docker Stack..."

# 1. Create a shared network
docker network create sql-monitor-net || true

# 2. Stop existing containers to re-create them with network
docker stop prometheus grafana || true
docker rm prometheus grafana || true

# 3. Run Prometheus on the network
# We still need host.docker.internal to reach the Mac host
docker run -d \
  --name prometheus \
  --network sql-monitor-net \
  -p 9090:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  --add-host host.docker.internal:host-gateway \
  prom/prometheus

# 4. Run Grafana on the network
docker run -d \
  --name grafana \
  --network sql-monitor-net \
  -p 3000:3000 \
  grafana/grafana-enterprise

echo "---------------------------------------------------"
echo "Stack is running!"
echo "1. Open Grafana: http://localhost:3000 (admin/admin)"
echo "2. Add Data Source (Prometheus):"
echo "   URL: http://prometheus:9090"
echo "   (Note: 'prometheus' is the container name, available on the shared network)"
echo "---------------------------------------------------"
