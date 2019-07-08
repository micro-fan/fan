FROM ubuntu:bionic

RUN apt-get update && \
    apt-get install -y git \
                       python3-pip

WORKDIR /code

ADD requirements.txt /code/requirements.txt
ADD requirements-dev.txt /code/requirements-dev.txt
RUN pip3 install -r requirements-dev.txt


ADD . /code
WORKDIR /code
