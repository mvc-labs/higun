FROM ubuntu:latest

WORKDIR /app
RUN echo "deb http://mirrors.aliyun.com/ubuntu/ jammy main \n" >> /etc/apt/sources.list
RUN apt-get update
RUN apt-get -y --no-install-recommends install libc6
RUN apt-get -y --no-install-recommends install libzmq3-dev
EXPOSE 8080

ENTRYPOINT ["/app/utxo_indexer"]