FROM docker.turn.com/tops-base/jdk7:latest
MAINTAINER instrumentation@turn.com

RUN	      mkdir -p /usr/share/opentsdb/static/
RUN	      mkdir -p /usr/share/opentsdb/resources/
RUN	      mkdir -p /data/opentsdb/cache

ENV	      PORT 8000
ENV	      CLASSPATH /usr/share/opentsdb
ENV	      BASE /usr/share/opentsdb

ADD	      tsdb-query-all* /usr/share/opentsdb/
ADD	      runner.sh /usr/share/opentsdb/
ADD	      resources /usr/share/opentsdb/resources/

CMD	      cd $BASE && ${BASE}/runner.sh ${PORT-8000}
