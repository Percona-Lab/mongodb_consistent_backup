FROM python:2.7
MAINTAINER Tim Jones <tim@spotahome.com>

RUN mkdir /app
COPY . /app
WORKDIR /app

RUN make PYTHON_BIN=/usr/local/bin/python \
 && make install

ENTRYPOINT [ "/usr/local/bin/python", "/usr/local/bin/mongodb-consistent-backup" ]
