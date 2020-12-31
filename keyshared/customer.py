#!/usr/bin/env python
# coding: utf-8 
# Gao Ming Ming Create At 2020-12-31 10:39:18
# Description:some description
import pulsar
from pulsar import ConsumerType
import sys
sys.path.append("..")
sys.path.append("../../../lib")
import env
import log
import threading
logger = log.GetLogger('SharedCustomer')

client = pulsar.Client(env.server)
def customer(topic,thread_index):
    consumer = client.subscribe(
        topic,
        subscription_name=topic,
        consumer_type=ConsumerType.KeyShared
    )

    while True:
        msg = consumer.receive()
        if thread_index is not None:
            logger.info("{}:{}".format(thread_index,msg.data()))
        else:
            logger.info(msg.data())
        consumer.acknowledge(msg)

def run():
    customer_type = ConsumerType.KeyShared
    topic = "key_shared_topic_test"
    thread_length = 10
    for i in range(thread_length):
        threading.Thread(
            target = customer,
            args = (topic,),
            kwargs = {"customer_type":customer_type,"thread_index":i}
        ).start()


if __name__ == "__main__":
    run()
