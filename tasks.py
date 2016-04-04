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
from kafka import KafkaConsumer, KafkaProducer, KafkaClient
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
        print("initializing kafka source")
        consumer = KafkaConsumer(bootstrap_servers=kafka_URI, auto_offset_reset='earliest', enable_auto_commit=False)
        consumer.subscribe([topic_str])
        print consumer.topics()
        print("successfully obtained the client")
        self.consumer = consumer
        print("consumer assigned")
        producer = KafkaProducer(bootstrap_servers=kafka_URI)
        producer.send(topic_str, "8/3/2015 10:32, P0001,1,0.0001,192,-4.1158,192,3.8264,192,0.0251,192,0.632,192,1.2619,192,5.042,192,14,192,1.7709,192,31,192,1.2619,192,45,192,0.6668,192,60,192,0.6737,192,73,192,0.4919,192,91,192,0.7214,192,104,192,0.2261,192,121,192,0.4308,192,137,192,0.2886,192,147,192,0.2891,192,166,192,0.382,192,178,192,0.29,192,197,192,0.2198,192,212,192,0.1568,192,227,192,0.2685,192,238,192,0.1496,192,257,192,0.1584,192,272,192,0.1844,192,285,192,0.1237,192,299,192,0.1162,192,317,192,0.2689,192,328,192,0.138,192,347,192,0.1172,192,358,192,0.1615,192,377,192,0.171,192,392,192,0.1877,192,404,192,0.1415,192,418,192,0.1555,192,432,192,0.1298,192,450,192,0.166,192, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null" )
        producer.send(topic_str, "8/3/2015 10:32, P0001,2,-0.0004,192,-3.6543,192,3.2065,192,0.0431,192,0.6258,192,2.3207,192,6.9022,192,12,192,3.787,192,29,192,2.3207,192,45,192,1.3414,192,58,192,0.9774,192,73,192,0.4887,192,90,192,0.8065,192,102,192,0.6761,192,118,192,0.3439,192,132,192,0.5097,192,149,192,0.252,192,166,192,0.536,192,182,192,0.2268,192,197,192,0.4055,192,210,192,0.2053,192,222,192,0.2471,192,240,192,0.2316,192,254,192,0.2765,192,272,192,0.1925,192,282,192,0.2358,192,297,192,0.1036,192,313,192,0.2268,192,330,192,0.0815,192,347,192,0.1517,192,357,192,0.1381,192,373,192,0.1562,192,388,192,0.1469,192,402,192,0.1649,192,421,192,0.1339,192,435,192,0.2961,192,450,192,0.1791,192, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null" )
        producer.send(topic_str, "8/3/2015 10:32, P0001,3,0,192,-0.0394,192,0.0435,192,0.0004,192,0.0048,192,0.0205,192,0.0546,192,12,192,0.0212,192,29,192,0.0205,192,43,192,0.0065,192,57,192,0.0091,192,74,192,0.0088,192,92,192,0.0043,192,105,192,0.006,192,120,192,0.0041,192,136,192,0.0025,192,150,192,0.0031,192,163,192,0.0031,192,178,192,0.002,192,192,192,0.0028,192,212,192,0.0021,192,224,192,0.0012,192,240,192,0.0015,192,257,192,0.0015,192,268,192,0.0016,192,284,192,0.0014,192,299,192,0.0015,192,313,192,0.0015,192,332,192,0.0011,192,343,192,0.0011,192,362,192,0.0016,192,373,192,0.001,192,391,192,0.0012,192,402,192,0.0015,192,417,192,0.0008,192,432,192,0.0011,192,452,192,0.0009,192, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null" )
        print(" data sent")


    def execute(self):
        sys.stderr.write("Thread started")
        for message in self.consumer:
            if message is not None:
                #print("Message received: %s" %message.value)
                print("%s:%d:%d: key=%s value=%s" %(message.topic, message.partition, message.offset, message.key, message.value))
                result = executedag(message.value, 1, len(dag)-1)
                print("result is %s" % result)
                #execute the last sink task on this message
                if (result != None):
                    print("about to execute sink task")
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
        print("producer initialized")

    def execute(self, data):
        print("sending data")
        self.producer.send(self.topic, bytes(data))
        print("Sent")








