applications:
- name: sp
  command: python2.7 ./scoring_pipelines/scoringExecutor.py
  memory: 512M
  disk_quota: 1G
  timeout: 180
  instances: 1
services:
- zookeeper-for-sp
- kafka-for-sp
