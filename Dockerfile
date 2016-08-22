FROM ubuntu:xenial

RUN apt-get install -y -force-yes python3-pip \
                                  python3.5

RUN update-alternatives --install /usr/bin/python3 python3.5 /usr/bin/python3.5 0
ADD . /code
RUN pip install -r /code/requirements.txt
WORKDIR /code
ENTRYPOINT green