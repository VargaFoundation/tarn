#!/bin/bash

# Configuration
# In a real environment, you would discover the AM host via YARN ResourceManager API
AM_HOST=${1:-"localhost"}
AM_PORT=${2:-8888}
HAPROXY_SOCKET=${3:-"/var/run/haproxy.sock"}
BACKEND_NAME="triton_backend"

echo "Starting HAProxy update script..."
echo "AM Endpoint: http://${AM_HOST}:${AM_PORT}/instances"

while true; do
    # Fetch instance list from ApplicationMaster
    INSTANCES=$(curl -s http://${AM_HOST}:${AM_PORT}/instances)
    
    if [ $? -eq 0 ] && [ ! -z "$INSTANCES" ]; then
        echo "Active instances discovered: "
        echo "$INSTANCES"
        
        # Real-world logic would involve:
        # 1. Parsing the instance list (host:port)
        # 2. Using HAProxy Runtime API (via socat) to add/remove/update servers
        # Example using socat:
        # echo "add server ${BACKEND_NAME}/new_node addr 10.0.0.1 port 8000" | socat stdio ${HAPROXY_SOCKET}
        # For now, we just log the discovery.
    else
        echo "No instances found or AM unreachable."
    fi
    
    sleep 30
done
