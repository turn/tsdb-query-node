FROM docker.turn.com/tops-base/jdk7:latest
MAINTAINER instrumentation@turn.com

RUN	      mkdir -p /usr/share/opentsdb/static/
RUN	      mkdir -p /usr/share/opentsdb/resources/
RUN	      mkdir -p /data/opentsdb/cache

RUN 	  mkdir -p /var/log/opentsdb

ENV	      CLASSPATH  /usr/share/opentsdb
ENV	      BASE       /usr/share/opentsdb
ENV	      TSDB_PORT  4244

ADD	      tsdb-query-all* /usr/share/opentsdb/
ADD	      runner.sh /usr/share/opentsdb/
ADD	      resources /usr/share/opentsdb/resources/

WORKDIR   /usr/share/opentsdb/

CMD	      ./runner.sh