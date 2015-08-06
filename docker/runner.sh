#!/bin/bash

# runs the java class provided as parameter to this shell script.

PORT=$1

[[ -z "${PORT}" ]] && echo -e 'Did not specify port number. ' '\n' 'Usage: runner.sh PORT' && exit

BASE=`pwd`

echo ${BASE}

java -server -classpath ${BASE}/src/main/resources/:${BASE}/target/dependency/*:${BASE}/target/tsdb-query-0.1.jar net.opentsdb.tools.TSDMain --port ${PORT} --config=${BASE}/src/main/resources/opentsdb.conf
