FROM tapimages:8080/tap-base-python:python2.7-jessie

RUN mkdir -m 0775 -p /usr/src/app
WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y python-pip

ADD . /usr/src/app

EXPOSE 80

RUN pip install -r requirements.txt

ENV PORT 80

RUN env

CMD ["python", "./scoring_pipelines/scoringExecutor.py"]

