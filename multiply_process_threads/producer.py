#!/usr/bin/env python
# coding: utf-8 
# Gao Ming Ming Create At 2020-12-31 10:50:36
# Description:some description
import threading
import random
import pulsar
import env
import sys
sys.path.append("..")
sys.path.append("../../lib")
import log
client = pulsar.Client(env.server)
logger = log.GetLogger('MultiplyCustomer')

def producer(topic,value):
    '''
        生产者
    '''
    producer = client.create_producer(topic)
    producer.send(value.encode('utf-8'))
    producer.close()

def get_value():
    '''
        获得随机长度字符串
    '''
    string =''.join(random.sample(['z','y','x','w','v','u','t','s','r','q','p','o','n','m','l','k','j','i','h','g','f','e','d','c','b','a'], 23))* random.random(1,100)
    return string

def run():
    thread_count = 3000
    for i in range(thread_count):
        value = get_value()
        topic = "multiply_topic_test_thread{}_".format(i)

        value = topic + value
        threading.Thread(
          target = producer,
          args = (topic,value),
        ).start()


if __name__ == "__main__":
    run()
