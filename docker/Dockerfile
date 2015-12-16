FROM docker.turn.com/tops-base/jdk7:latest
MAINTAINER instrumentation@turn.com

ENV       VERSION 0.1.1-2015121504
ENV       WORKDIR /usr/share/opentsdb
ENV       LOGDIR  /var/log/opentsdb
ENV       DATADIR /data/opentsdb

RUN	      mkdir -p $WORKDIR/static
RUN	      mkdir -p $WORKDIR/resources
RUN	      mkdir -p $DATADIR/cache
RUN 	    mkdir -p $LOGDIR
RUN 	    mkdir -p /etc/opentsdb

ENV	      CLASSPATH  $WORKDIR
ENV	      TSDB_PORT  4244
ENV       ENABLE_SEARCH false

WORKDIR   $WORKDIR

ADD       tsdb-query-all* $WORKDIR/
ADD       resources $WORKDIR/resources/
ADD       plugins $WORKDIR/plugins/

VOLUME ["/etc/opentsdb"]

ENTRYPOINT /bin/bash -c "java -server -classpath $WORKDIR/resources:$WORKDIR:$WORKDIR/tsdb-query-all-$VERSION.jar net.opentsdb.tools.TSDMain --port $TSDB_PORT --read-only --config=/etc/opentsdb/opentsdb.conf --enable-search=${SEARCH}"


