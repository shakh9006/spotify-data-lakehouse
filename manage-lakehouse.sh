#!/bin/bash
# Local Lakehouse Management Script
# 
# This script orchestrates the startup and shutdown of a complete data lakehouse stack:
# - Simulator (Kafka producer)
# - Kafka (Message broker)
# - Kafdrop (Kafka UI)
#
# Usage: ./manage-simulator.sh [start|stop]

set -e  # Exit immediately if any command fails

# Get the absolute path of the script directory to ensure relative paths work correctly
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Function to start all lakehouse services in the correct order
start_services() {
    echo "Starting Lakehouse and Simulator services..."
    
    # Change to script directory to ensure docker-compose files are found
    cd "$SCRIPT_DIR"
    
    # Step 1: Start the simulator and Kafka services (Simulator + Kafka + Kafdrop)
    echo "Starting simulator and Kafka services..."
    docker compose -f ./simulator/docker-compose.yaml up -d --build

    sleep 5  # Allow services to initialize

    # Step 2: Start the storage services (Minio + Nessie)
    echo "Starting storage services..."
    docker compose -f ./storage/docker-compose.yaml up -d --build

    sleep 5  # Allow services to initialize

    # Step 3: Start the spark streaming services (Spark + Spark Master + Spark Worker)
    echo "Starting spark streaming services..."
    docker compose -f ./spark-streaming/docker-compose.yaml up -d --build

    sleep 5  # Allow services to initialize

    # Step *: Create Network if it doesn't exist
    if ! docker network ls | grep -q spotify-analytics; then
        docker network create spotify-analytics

        docker network connect spotify-analytics broker
        docker network connect spotify-analytics zookeeper
        docker network connect spotify-analytics kafdrop
        docker network connect spotify-analytics simulator
        docker network connect spotify-analytics minio
        docker network connect spotify-analytics nessie
        docker network connect spotify-analytics spark-master
        docker network connect spotify-analytics spark-worker

    fi
    
    echo "All services started successfully."
    echo ""
    echo "Service Access Information:"
    echo "  - Simulator: http://localhost:8000"
    echo "  - Kafka: http://localhost:9092"
    echo "  - Kafdrop: http://localhost:9009"
    echo "  - Minio: http://localhost:9000"
    echo "  - Nessie: http://localhost:19120"
    echo "  - Spark Master: http://localhost:8088"
    echo "  - Spark Worker: http://localhost:8081"
    echo ""
}

# Function to stop all lakehouse services and clean up resources
stop_services() {
    echo "Stopping Lakehouse and Simulator services..."
    
    # Change to script directory
    cd "$SCRIPT_DIR"
    
    # Stop services in reverse order (Simulator -> Kafka -> Kafdrop -> Spark Streaming -> Storage)
    # The -v flag removes associated volumes to ensure clean shutdown
    echo "Stopping simulator and Kafka services..."
    docker compose -f ./simulator/docker-compose.yaml down -v

    echo "Stopping storage services..."
    docker compose -f ./storage/docker-compose.yaml down -v
    
    echo "Stopping spark streaming services..."
    docker compose -f ./spark-streaming/docker-compose.yaml down -v

    echo "All services stopped and volumes cleaned up."
    echo ""
}

# Main script logic - handle command line arguments
case "${1:-help}" in
    "start")
        start_services
        ;;
    "stop")
        stop_services
        ;;
    *)
        echo "Lakehouse and Simulator Management Script"
        echo ""
        echo "Usage: $0 [start|stop]"
        echo ""
        echo "Commands:"
        echo "  start    Start all lakehouse services (Simulator, Kafka, Kafdrop, Spark Streaming, Storage)"
        echo "  stop     Stop all services and clean up volumes"
        echo ""
        echo "Examples:"
        echo "  $0 start    # Start the complete lakehouse stack"
        echo "  $0 stop     # Stop all services and clean up"
        echo ""
        echo "After starting, you can access:"
        echo "  - Simulator: http://localhost:8000"
        echo "  - Kafka: http://localhost:9092"
        echo "  - Kafdrop: http://localhost:9009"
        echo "  - Minio: http://localhost:9000"
        echo "  - Nessie: http://localhost:19120"
        echo "  - Spark Master: http://localhost:8088"
        echo "  - Spark Worker: http://localhost:8081"
        ;;
esac