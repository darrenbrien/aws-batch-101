FROM python:3.7.12-slim-buster

RUN apt-get update \
    && apt-get install -y awscli \
    && apt-get install -y zip

RUN mkdir /opt/app
COPY requirements.txt /opt/app
WORKDIR /opt/app
RUN pip install -r requirements.txt
COPY fetch_and_run.sh /opt/app
RUN chmod 777 /opt/app/fetch_and_run.sh

ENTRYPOINT ["/opt/app/fetch_and_run.sh"]
