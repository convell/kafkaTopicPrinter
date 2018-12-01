""" Main Docstring:
        File:kafkaTopicPrinter.py
        Version:1.00 (Paul Marshall)
            Made the File to learn kafka
        Vital non standard imports: pykafka
        Goals:
            Uses Kafka topics as input
            Outputs to std (and eventually kafka)
            Make a clean readable document
        """
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import threading
import time
import sys
import Queue
import json
from pykafka import KafkaClient, common

def main():
    client = KafkaClient(hosts = 'YOURDB')
    print "Here are the topics available to print:"
    print client.topics

    option = raw_input("What topic would you like to print: ")
    #print option

    topic = client.topics[option]

    consumer = topic.get_balanced_consumer(
        consumer_group = 'YOURGROUP',
        zookeeper_connect = 'YOURZK',
        auto_offset_reset = common.OffsetType.EARLIEST, #change to latest for 'real time' streams
        reset_offset_on_start = True
    )
    
    while True:
        try:
            myString = consumer.consume()
            outputData = json.loads(myString.value)
            print outputData
        except KeyboardInterrupt:
            break
        
    try:
        print consumer.consume()
    except KeyboardInterrupt:
        sys.exit

if __name__ == "__main__":
    main()
