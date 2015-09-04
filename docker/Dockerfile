FROM ubuntu:latest
MAINTAINER arjun.satish@turn.com

# Install some basic utils
RUN           sudo apt-get update
RUN           sudo apt-get upgrade -y
RUN           sudo apt-get install -y software-properties-common

# Install Java
# Java should be installed before anything else to avoid version problems
RUN           sudo apt-get install -y openjdk-7-jre openjdk-7-jdk
RUN           sudo apt-get install -y vim

RUN	      mkdir -p /usr/share/opentsdb/static/
RUN	      mkdir -p /usr/share/opentsdb/resources/
RUN	      mkdir -p /data/opentsdb/cache

ENV	      PORT 8000
ENV	      CLASSPATH /usr/share/opentsdb
ENV	      BASE /usr/share/opentsdb

ADD	      tsdb-query-all* /usr/share/opentsdb/
ADD	      runner.sh /usr/share/opentsdb/
ADD	      *.sh logback.xml opentsdb.conf /usr/share/opentsdb/resources/

CMD	      cd $BASE && ${BASE}/runner.sh ${PORT-8000}

