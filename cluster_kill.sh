#!/bin/bash
NIMBUS_HOST='172.17.24.217'
TOPOLOGY_NAME='kafka_spout_example'
pyleus kill -n $NIMBUS_HOST $TOPOLOGY_NAME