FROM ubuntu:xenial

RUN apt-get update && \
    apt-get install -y git \
                       python3-pip \
                       python3.5

RUN update-alternatives --install /usr/bin/python3 python3.5 /usr/bin/python3.5 0

WORKDIR /code

ADD requirements.txt /code/requirements.txt
ADD requirements-dev.txt /code/requirements-dev.txt
RUN pip3 install -r requirements-dev.txt


ADD . /code
WORKDIR /code
