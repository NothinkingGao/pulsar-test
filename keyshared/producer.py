#!/usr/bin/env python
# coding: utf-8 
# Gao Ming Ming Create At 2020-12-31 10:27:02
# Description:some description
import pulsar
import random
import threading
import sys
sys.path.append("../")
sys.path.append("../../../lib")
import env
import log
logger = log.GetLogger('SharedKey')

client = pulsar.Client(env.server)
def producer(topic,partition_key = None):
    '''
        生产者
    '''
    producer = client.create_producer(topic)
    if partition_key:
        logger.info("partition key:"+partition_key)
        producer.send(("message from producer"+partition_key).encode('utf-8'),partition_key = partition_key)
    producer.send(topic.encode('utf-8'))
    producer.close()


def run():
    topic = "key_shared_topic_test"
    thread_length = 10
    for i in range(thread_length):
        key = random.sample(range(thread_length),1)[0]
        threading.Thread(
            target = producer,
            args = (topic,),
            kwargs = {"partition_key":str(key)}
        ).start()


if __name__ == "__main__":
    run()
