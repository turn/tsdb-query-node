#!/bin/bash

# runs the java class provided as parameter to this shell script.

PORT=$1

[[ -z "${PORT}" ]] && echo -e 'Did not specify port number. ' '\n' 'Usage: runner.sh PORT' && exit

BASE=`pwd`

echo "Working directory" ${BASE}

java -server -classpath /usr/share/opentsdb/resources/:${BASE}/tsdb-query-all-0.1.1.jar net.opentsdb.tools.TSDMain --port ${PORT} --config=${BASE}/resources/opentsdb.conf
