#!/bin/bash
set -e  # Exit on command errors
set -x  # Print each command before execution, useful for debugging
source ".venv/bin/activate"
WORKLOAD=$1
eth0Ip=$(ifconfig eth0 | sed -En -e 's/.*inet ([0-9.]+).*/\1/p')
export HOST_IP="$eth0Ip"
export PRIMARYRAFTSTATEFILE='/qdrantprimarystorage/raft_state.json'
echo "WORKLOAD: $WORKLOAD"
if [ "$WORKLOAD" == "qdrantprimary" ];
then
    if [ -z "${QDRANT_PRIMARY_ADDRESS}" ]; then
        echo "Error: QDRANT_PRIMARY_ADDRESS not set"
        exit 1
    fi
    ./qdrant --uri "${QDRANT_PRIMARY_ADDRESS}" --disable-telemetry
elif [ "$WORKLOAD" == "qdrantsecondary" ];
then
    if [ -z "${QDRANT_PRIMARY_ADDRESS}" ]; then
        echo "Error: QDRANT_PRIMARY_ADDRESS not set"
        exit 1
    fi
    if [ -z "${QDRANT_SECONDARY_ADDRESS}" ]; then
        echo "Error: QDRANT_SECONDARY_ADDRESS not set"
        exit 1
    fi
    if [ -z "${QDRANT_PRIMARY_REST_ADDRESS}" ]; then
        echo "Error: QDRANT_PRIMARY_REST_ADDRESS not set"
        exit 1
    fi
    okay_string="ok"
    while [ true ]
    do
        status=$(curl $QDRANT_PRIMARY_REST_ADDRESS/cluster | jq -r '.status')
        if test "$status" = "$okay_string"; then
            break
        fi
        echo "primary not ready..."
        sleep 10
    done
    echo "primary is ready now... starting secondary"
    ./qdrant --bootstrap "${QDRANT_PRIMARY_ADDRESS}" --uri "${QDRANT_SECONDARY_ADDRESS}" --disable-telemetry
fi
tail -f /dev/null