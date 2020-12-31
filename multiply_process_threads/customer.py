#!/usr/bin/env python
# coding: utf-8 
# Gao Ming Ming Create At 2020-12-31 11:20:28
# Description:some description
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
        consumer_type = ConsumerType.Exclusive
    )

    while True:
        msg = consumer.receive()
        logger.info(msg.data())
        consumer.acknowledge(msg)


def run():
    thread_count = 3000
    for i in range(thread_count):
        topic = "multiply_topic_test_thread{}_".format(i)

        threading.Thread(
          target = customer,
          args = (topic,),
        ).start()


if __name__ == "__main__":
    run()
