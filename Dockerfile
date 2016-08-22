FROM python

ADD . /code
RUN pip install -r /code/requirements.txt
WORKDIR /code
ENTRYPOINT green