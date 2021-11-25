from public.ecr.aws/docker/library/python:3.8-slim

RUN mkdir /opt/app
COPY . /opt/app
WORKDIR /opt/app
RUN pip install -r requirements.txt
