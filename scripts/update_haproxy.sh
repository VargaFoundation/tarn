#!/bin/bash

# Configuration
APP_TAG="TARN"
APP_NAME="Triton-on-YARN"
AM_PORT=8888
HAPROXY_SOCKET=${1:-"/var/run/haproxy.sock"}
TARN_TOKEN=${2:-""}
BACKEND_NAME="triton_backend"
CHECK_INTERVAL=30

AM_HOST=""

# Function to discover Application Master Host using YARN CLI
discover_am_host() {
    echo "Attempting to discover AM host via YARN CLI..."
    # List running applications with the specific tag and name
    # We use awk to find the line with our app name and extract the Tracking-URL
    TRACKING_URL=$(yarn application -list -appTags ${APP_TAG} 2>/dev/null | grep "${APP_NAME}" | grep "RUNNING" | awk '{print $9}')
    
    if [ -z "$TRACKING_URL" ]; then
        # Try without tags if tags failed or aren't supported by this YARN version
        TRACKING_URL=$(yarn application -list 2>/dev/null | grep "${APP_NAME}" | grep "RUNNING" | awk '{print $9}')
    fi

    if [ ! -z "$TRACKING_URL" ]; then
        # Tracking URL is usually http://host:port/ or http://host:port
        # Extract host
        AM_HOST=$(echo $TRACKING_URL | sed -e 's|http://||' -e 's|:.*||' -e 's|/.*||')
        # Also extract port from Tracking URL if possible
        DISCOVERED_PORT=$(echo $TRACKING_URL | sed -e 's|http://||' | grep ":" | sed -e 's|.*:||' -e 's|/.*||')
        if [ ! -z "$DISCOVERED_PORT" ]; then
            AM_PORT=$DISCOVERED_PORT
        fi
        echo "Discovered AM Host: ${AM_HOST} on port ${AM_PORT}"
    else
        echo "Failed to discover AM Host. Ensure the application is running."
        AM_HOST=""
    fi
}

update_haproxy() {
    local instances=$1
    echo "Updating HAProxy with instances: ${instances}"
    # Logic to update HAProxy via Runtime API (socat)
}

echo "Starting HAProxy dynamic update script..."

while true; do
    if [ -z "$AM_HOST" ]; then
        discover_am_host
    fi

    if [ ! -z "$AM_HOST" ]; then
        echo "Querying AM at http://${AM_HOST}:${AM_PORT}/instances"
        
        CURL_OPTS="-s --max-time 5"
        if [ ! -z "$TARN_TOKEN" ]; then
            CURL_OPTS="${CURL_OPTS} -H \"X-TARN-Token: ${TARN_TOKEN}\""
        fi
        
        # Execute curl with evaluated options
        INSTANCES=$(eval curl ${CURL_OPTS} http://${AM_HOST}:${AM_PORT}/instances)
        
        if [ $? -eq 0 ] && [ ! -z "$INSTANCES" ]; then
            # Verify if we got an error message instead of instances (e.g. 401 Unauthorized)
            if [[ "$INSTANCES" == *"Unauthorized"* ]]; then
                 echo "Error: Unauthorized access to AM. Check your token."
                 sleep ${CHECK_INTERVAL}
                 continue
            fi
            update_haproxy "$INSTANCES"
        else
            echo "AM at ${AM_HOST} is unreachable or returned no instances. Retrying discovery..."
            AM_HOST=""
            continue
        fi
    else
        echo "Waiting for AM to be available..."
    fi

    sleep ${CHECK_INTERVAL}
done
