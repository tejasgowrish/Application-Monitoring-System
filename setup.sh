#!/bin/bash

# Ensure we're running in the project root directory
cd ~/app-monitoring

echo "Setting up Application Monitoring Dashboard project..."

# Create directories if they don't exist
mkdir -p api loggen kafka/prometheus kafka/loki grafana/provisioning/datasources grafana/provisioning/dashboards

echo "Creating API server files..."
# Copy API files
cp -v requirements.txt api/
cp -v Dockerfile api/
cp -v main.py api/

echo "Creating request generator files..."
# Copy request generator files
cp -v request_generator.py loggen/
cp -v loggen/Dockerfile loggen/

echo "Creating Kafka consumer files..."
# Copy Kafka consumer files
cp -v log_consumer.py kafka/
cp -v consumer_dockerfile kafka/Dockerfile

echo "Setting up Prometheus configuration..."
# Create Prometheus configuration
mkdir -p prometheus
cp -v prometheus.yml prometheus/

echo "Setting up Grafana configuration..."
# Create Grafana configuration directories
mkdir -p grafana/provisioning/datasources grafana/provisioning/dashboards

# Copy Grafana configuration files
cp -v datasources.yml grafana/provisioning/datasources/
cp -v dashboards.yml grafana/provisioning/dashboards/
cp -v api-monitoring.json grafana/provisioning/dashboards/

echo "Starting the application stack..."
# Run docker-compose
docker-compose up -d

echo "Application stack is starting up..."
echo "The Grafana dashboard will be available at http://localhost:3000"
echo "Login with admin/admin"
echo "The API server will be available at http://localhost:8000"

echo "Setup complete!"