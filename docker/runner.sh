#!/bin/bash

# runs the java class provided as parameter to this shell script.

PORT=$TSDB_PORT

[[ -z "${PORT}" ]] && echo -e 'Port number is not set in TSDB_PORT. Using default 4244' && PORT='4244'

BASE=`pwd`

echo "Working directory" ${BASE}, 'PORT=' $PORT

java -server -classpath /usr/share/opentsdb/resources/:${BASE}/tsdb-query-all-0.1.1.jar net.opentsdb.tools.TSDMain --port ${PORT} --config=${BASE}/resources/opentsdb.conf
