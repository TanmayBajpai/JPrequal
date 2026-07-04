#!/bin/bash

BASE_PORT=9001
COUNT=${1:-8}

for i in $(seq 0 $((COUNT - 1))); do
    port=$((BASE_PORT + i))
    mvn exec:java -pl backend-sim -Dexec.mainClass="io.jprequal.sim.BackendServer" -Dexec.args="$port" &
done

wait