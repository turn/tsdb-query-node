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
RUN	      mkdir -p /data/opentsdb/cache

EXPOSE	      8000
