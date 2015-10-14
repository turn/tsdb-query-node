#!/bin/bash

# runs the java class provided as parameter to this shell script.

PORT=$TSDB_PORT
SEARCH=$ENABLE_SEARCH

[[ -z "${PORT}" ]] && echo -e 'Port number is not set in TSDB_PORT. Using default 4244' && PORT='4244'
[[ -z "${SEARCH}" ]] && echo -e 'Search is not set in ENABLE_SEARCH. Using default false' && SEARCH=false


BASE=`pwd`

echo "Working directory" ${BASE}, 'PORT=' $PORT, 'SEARCH=' $SEARCH,

java -server -classpath /usr/share/opentsdb/resources/:${BASE}/tsdb-query-all-0.1.1-07oct15.jar net.opentsdb.tools.TSDMain --port ${PORT} --config=${BASE}/resources/opentsdb.conf --enable-search=${SEARCH}