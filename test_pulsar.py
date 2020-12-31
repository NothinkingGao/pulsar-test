#!/usr/bin/env python
# coding: utf-8 
# Gao Ming Ming Create At 2020-12-17 17:29:54
# Description:some description

import pulsar
from pulsar import ConsumerType
import time
import random
import sys
import math
import click
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
def customer(topic,customer_type = None,thread_index=None):
    '''
        消费者
    '''
    if not customer_type:
        customer_type = ConsumerType.Exclusive

    consumer = client.subscribe(
        topic,
        subscription_name=topic,
        consumer_type=customer_type
    )
    
    while True:
        msg = consumer.receive()
        if thread_index is not None:
            logger.info("{}:{}".format(thread_index,msg.data()))
        else:
            logger.info(msg.data())
        consumer.acknowledge(msg)

def producer(topic,partition_key=None):
    '''
        生产者
    '''
    producer = client.create_producer(topic)
    if partition_key:
        logger.info("partition key:"+partition_key)
        producer.send(("message from producer"+partition_key).encode('utf-8'),partition_key = partition_key)
    producer.send(topic.encode('utf-8'))
    producer.close()
    

def multiy_threads(process_index,count,fn,topic = None):
    '''
        多线程
    '''
    results = []
    for i in range(process_index*count,(process_index+1)*count):
        topic = "pulsar_process{}_thread{}".format(process_index,i)
        results.append(topic)
    logger.info(results)
    
    threads = []
    for item in results:
        t=threading.Thread(target = fn,args = (item,))
        t.start()
        threads.append(t)
    
    for item in threads:
        item.join()



def multiply_process(process,count,fn):
    print("multiply_process")
    average_count = int(math.ceil(count/process))
    client = pulsar.Client(env.server)
    progresses = [multiprocessing.Process(target = multiy_threads,args = (i,average_count,fn)) for i in range(process)]

    for item in progresses:
        item.start()

    for item in progresses:
        item.join()


def single_topic(c = None,s = None):
    '''
        1.单一主题
        2.单一消费者
        3.多线程并发生产消息
    '''
    if c and s:
        print("customer and producer can not run in one progress.")
        return
    topic = "single_topic"
    if c:
        customer(topic)
    if s:
        multiy_threads(1,s,producer,topic)


def muliply_customer_with_muliply_producer(c = None,s = None,p=None):
    '''
        1.多个消费者
        2.多个主题
        3.多个生产者
    '''
    if c and s:
        print("customer and producer can not run in one progress.")
        return
    if c:
        multiply_process(p,c,customer)
    if s:
        multiply_process(p,s,producer)
   

def multiply_customer(c = None,s = None):
    '''
        1.多个生产者,指定消费key
        2.多个消费者,消费模式为key_shared
        3.单一主题,测试key相同时是否保序
    '''
    if c and s:
        print("customer and producer can not run in one progress.")
        return
    topic = "key_topic_test"

    if c:
        customer_type = ConsumerType.KeyShared
        for i in range(c):
            threading.Thread(
                target = customer,
                args = (topic,),
                kwargs = {"customer_type":customer_type,"thread_index":i}
            ).start()
    if s:
        for i in range(s):
            key = random.sample(range(s),1)[0]
            threading.Thread(
                target = producer,
                args = (topic,),
                kwargs = {"partition_key":str(key)}
            ).start()


@click.command()
@click.option('-option',default="",help="测试的类型")
@click.option('-customer',default=None,help="客户端数量")
@click.option('-producer',default=None,help="服务端数量")
@click.option('-progress',default=None,help="进程数")
@click.option('-thread',default=None,help="线程数")
def main(option,customer,producer,progress,thread):
    customer = int(customer) if customer else None
    producer = int(producer) if producer else None
    progress = int(progress) if progress else None
    thread   = int(thread) if thread else None

    if option == "test1":
        multiply_customer(customer,producer)
    elif option == "test2":
        muliply_customer_with_muliply_producer(customer,producer,progress)
    elif option == "test3":
        single_topic(customer,producer)
    print("please select the option.")

if __name__ == "__main__":
    #main()
    multiy_threads(1,5,producer)
