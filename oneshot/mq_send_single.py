#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import oneshot_common
import helper

import sys
import pika
import json
import logging
import datetime
import pymongo
import configparser
import time

#configs = helper.load_config("../ntrust.ini")
configs = helper.load_config("../dev.ini")

NTRUST_QNAME = "task"
NTRUST_XNAME = "notice"

def connect_mq():
    global connection, channel, channel_produce

    mq_uri = configs["RABBITMQ"]["uri"]
    print("RabbitMQ URI:", mq_uri)
    connection = pika.BlockingConnection(pika.connection.URLParameters(mq_uri))
    channel = connection.channel()
    channel_produce = connection.channel()

    channel.queue_declare(queue=NTRUST_QNAME, durable=True)
    channel_produce.exchange_declare(exchange=NTRUST_XNAME, type='fanout')


def send_to_worker(obj):
    print("Send Task:", obj)
    channel.basic_publish(exchange='', routing_key=NTRUST_QNAME,
                          properties=pika.BasicProperties(
                              delivery_mode = 2, # make message persistent
                          ),
                          body=json.dumps(obj))

def produce_notice(obj):
    print("Send Notice:", obj)
    json_txt = json.dumps(obj)
    channel_produce.basic_publish(exchange=NTRUST_XNAME, routing_key='', body=json_txt)



VALID_CMDS = [ "basic", "byline", "update", "ping", "exit", "score" ]

if len(sys.argv) > 1 and sys.argv[1] in VALID_CMDS:
    cmd = sys.argv[1]
    args = sys.argv[2:]

    connect_mq()

    if cmd == "byline" or cmd == "basic" or cmd == "score":
        if len(args) < 1:
            raise SystemExit("newsId argument required.")
        send_to_worker({ "cmd":cmd, "ver":1, "newsId":args[0] })
    elif cmd == "update":
        produce_notice({"cmd": "update", "ver": 1})
    elif cmd == "ping":
        produce_notice({"cmd": "ping", "ver": 1, "param": "something special"})
    elif cmd == "exit":
        produce_notice({"cmd": "exit", "ver": 1})
    else:
        print("Invalid cmd:", cmd)

    connection.close()

# 모든 news에 대해 basic 처리
if __name__ == '__main__':
    configs = helper.load_config("../dev.ini")
    helper.mongo.connect(configs["DEST_MONGODB"])
    coll_news = helper.mongo.get_collection("news")

    connect_mq()

    docs = coll_news.find({}, {"newsId": 1})  # .limit(1)
    for doc in docs:
        newsId = doc["newsId"]
        print(newsId)
        #send_to_worker({"cmd": "basic", "ver": 1, "newsId": newsId})

    print("send_basic_to_all: Done")