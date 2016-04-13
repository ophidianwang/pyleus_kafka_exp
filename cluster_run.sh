#!/bin/bash
NIMBUS_HOST='172.17.24.217'
cd $(dirname $0)
pyleus submit -n $NIMBUS_HOST kafka_spout_example.jar