#!/usr/bin/env python
import pika
import time
import json

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='nasacdf_task_queue', durable=True)
print ' [*] Waiting for messages. To exit press CTRL+C'

def callback(ch, method, properties, body):
    print body
    '''print(" [x] Received %r" % json.loads(body))
    print " [x] Received %r" % (body,)
    print "sleep for %d seconds" %(body.count('.'))
    time.sleep( body.count('.') )'''
    print " [x] Done"
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue='nasacdf_task_queue')

channel.start_consuming()
