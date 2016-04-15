# vim: set encoding=utf-8

#
#  Copyright (c) 2015 Intel Corporation 
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

"""
Python-scoring-dag-executor
"""
from kafka import KafkaConsumer, KafkaProducer 
import sys
import record

headers = {'Content-Type': 'application/json'}

nodes = []
dag = []
pymembers = []
uploaded = False

def  executedag(message, startindex, finishindex):
    i = startindex
    while i != finishindex:
        message = dag[i].execute(message)
        i += 1
    return message


class sourcetask(object):

    def __init__(self, kafka_URI,topic_str):
        consumer = KafkaConsumer(bootstrap_servers=kafka_URI, auto_offset_reset='earliest', enable_auto_commit=False)
        consumer.subscribe([topic_str])
        self.consumer = consumer

    def execute(self):
        sys.stderr.write("Thread started")
        for message in self.consumer:
            if message is not None:
               # print("%s:%d:%d: key=%s value=%s" %(message.topic, message.partition, message.offset, message.key, message.value))
                result = executedag(message.value, 1, len(dag)-1)
                #execute the last sink task on this message
                if (result != None):
                    dag[len(dag) -1].execute(result)

class generaltask(object):

    def __init__(self, file_name, user_func, schema):
        mod = __import__(file_name.split('.')[0])
        self.method_to_call = getattr(mod, user_func)
        self.schema = schema

    def execute(self, input):
        data = input.split(',')
        row = record.Record(self.schema)
        row._set_data(data)	
        return self.method_to_call(row)


class sinktask(object):

    def __init__(self, kafka_URI, topic_str):
        self.producer = KafkaProducer(bootstrap_servers=kafka_URI)
        self.topic = topic_str

    def execute(self, data):
        self.producer.send(self.topic, bytes(data))








