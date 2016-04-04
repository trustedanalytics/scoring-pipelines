applications:
- name: etlScoring
  command: python2.7 ./lib/scoringExecutor.py 
  memory: 128M
  disk_quota: 1G
  timeout: 180
  instances: 1
services:
- Iman-SpaceShuttleZookeeper 
- kafka-gateway-27d7e60e-8762-4661-a79e
