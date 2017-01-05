FROM python:2.7
MAINTAINER Tim Jones <tim@spotahome.com>

# Install mongodump - taken from official Docker Hub MongoDB Dockerfile
RUN apt-key adv --keyserver ha.pool.sks-keyservers.net --recv-keys 0C49F3730359A14518585931BC711F9BA15703C6
ENV MONGO_MAJOR=3.4 \
    MONGO_VERSION=3.4.1 \
    MONGO_PACKAGE=mongodb-org
RUN echo "deb http://repo.mongodb.org/apt/debian jessie/mongodb-org/$MONGO_MAJOR main" > /etc/apt/sources.list.d/mongodb-org.list
RUN set -x \
 && apt-get update \
 && apt-get install -y \
        ${MONGO_PACKAGE}-tools=$MONGO_VERSION \
 && rm -rf /var/lib/apt/lists/*

# Install application
RUN mkdir /app
COPY . /app
WORKDIR /app

RUN make PYTHON_BIN=/usr/local/bin/python \
 && make install

ENTRYPOINT [ "/usr/local/bin/python", "/usr/local/bin/mongodb-consistent-backup" ]
