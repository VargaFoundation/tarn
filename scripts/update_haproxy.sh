#!/bin/bash
#
# Copyright © 2008 Varga Foundation (contact@varga.org)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


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
    if [ -z "$instances" ]; then
        echo "No instances provided for update."
        return
    fi

    # Check if socat is available
    if ! command -v socat &> /dev/null; then
        echo "Error: socat is not installed. Please install it to use dynamic HAProxy updates."
        return
    fi

    # Convert newline-separated list to an array of desired instances
    mapfile -t desired_list <<< "$instances"
    local num_desired=${#desired_list[@]}
    echo "Updating HAProxy to match $num_desired instances"

    # Get current state from HAProxy using 'show servers state'
    # Format version 1: srv_name is $4, srv_addr is $5, srv_port is $19, srv_admin_state is $7
    local haproxy_data=$(echo "show servers state $BACKEND_NAME" | socat stdio unix-connect:"$HAPROXY_SOCKET" 2>/dev/null | grep -v "^#" | grep -v "^1$")
    
    if [ -z "$haproxy_data" ]; then
        echo "Warning: Could not retrieve servers state from HAProxy (backend: $BACKEND_NAME)."
        return
    fi

    # Arrays and Maps to track HAProxy state
    local all_slots=()
    declare -A slot_to_instance
    declare -A slot_admin_state

    while read -r line; do
        [ -z "$line" ] && continue
        local srv_name=$(echo "$line" | awk '{print $4}')
        local srv_addr=$(echo "$line" | awk '{print $5}')
        local srv_port=$(echo "$line" | awk '{print $19}')
        local admin_state=$(echo "$line" | awk '{print $7}')
        
        all_slots+=("$srv_name")
        slot_to_instance["$srv_name"]="$srv_addr:$srv_port"
        slot_admin_state["$srv_name"]="$admin_state"
    done <<< "$haproxy_data"

    declare -A used_slots
    local remaining_desired=()

    # 1. Step 1: Identify instances already correctly mapped to a READY slot (admin_state 0)
    for inst in "${desired_list[@]}"; do
        local matched=0
        for slot in "${all_slots[@]}"; do
            if [ -z "${used_slots[$slot]}" ] && [ "${slot_to_instance[$slot]}" == "$inst" ] && [ "${slot_admin_state[$slot]}" == "0" ]; then
                used_slots["$slot"]=1
                echo "Instance $inst is already active in slot $slot."
                matched=1
                break
            fi
        done
        if [ $matched -eq 0 ]; then
            remaining_desired+=("$inst")
        fi
    done

    # 2. Step 2: Identify instances already in a slot but currently in MAINT, and enable them
    local still_to_add=()
    for inst in "${remaining_desired[@]}"; do
        local matched=0
        for slot in "${all_slots[@]}"; do
            if [ -z "${used_slots[$slot]}" ] && [ "${slot_to_instance[$slot]}" == "$inst" ]; then
                echo "Enabling instance $inst in existing slot $slot."
                echo "set server $BACKEND_NAME/$slot state ready" | socat stdio unix-connect:"$HAPROXY_SOCKET" > /dev/null
                used_slots["$slot"]=1
                matched=1
                break
            fi
        done
        if [ $matched -eq 0 ]; then
            still_to_add+=("$inst")
        fi
    done

    # 3. Step 3: Assign completely new instances to unused slots and put redundant slots in MAINT
    local add_idx=0
    for slot in "${all_slots[@]}"; do
        if [ -z "${used_slots[$slot]}" ]; then
            if [ $add_idx -lt ${#still_to_add[@]} ]; then
                local inst="${still_to_add[$add_idx]}"
                local host=$(echo "$inst" | cut -d':' -f1)
                local port=$(echo "$inst" | cut -d':' -f2)
                
                echo "Assigning new instance $inst to slot $slot"
                # Update address and port
                echo "set server $BACKEND_NAME/$slot addr $host port $port" | socat stdio unix-connect:"$HAPROXY_SOCKET" > /dev/null
                # Set state to ready
                echo "set server $BACKEND_NAME/$slot state ready" | socat stdio unix-connect:"$HAPROXY_SOCKET" > /dev/null
                
                used_slots["$slot"]=1
                add_idx=$((add_idx+1))
            else
                # Slot is not needed, set to maintenance if it's currently active
                if [ "${slot_admin_state[$slot]}" != "1" ]; then
                    echo "Setting unused slot $slot to maintenance."
                    echo "set server $BACKEND_NAME/$slot state maint" | socat stdio unix-connect:"$HAPROXY_SOCKET" > /dev/null
                fi
            fi
        fi
    done

    if [ $add_idx -lt ${#still_to_add[@]} ]; then
        echo "Error: Not enough slots in HAProxy backend '$BACKEND_NAME' to accommodate all instances!"
        echo "Missing $(( ${#still_to_add[@]} - add_idx )) slots."
    fi
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
