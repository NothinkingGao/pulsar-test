#!/usr/bin/env python
# coding: utf-8 
# Gao Ming Ming Create At 2020-12-31 11:39:36
# Description:some description
import math
import click
import pulsar
from pulsar import ConsumerType
import sys
sys.path.append("..")
import env
import config as conf
import threading
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
sys.path.append("../../lib")
import log
logger = log.GetLogger('KafkaCustomer')

client = pulsar.Client(env.server)
def customer(topic):
    '''
        消费者
    '''
    consumer = client.subscribe(
        topic,
        subscription_name=topic,
        consumer_type=ConsumerType.Exclusive
    )

    while True:
        msg = consumer.receive()
        logger.info(msg.data())
        consumer.acknowledge(msg)

def run():
    topic = "single_topic_consumer"
    customer(topic)

if __name__ == "__main__":
    run()
