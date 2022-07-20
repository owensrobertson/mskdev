import logging
import threading
import os
import sys
import time
import json
import pymysql
import pandas as pd
from kafka import KafkaConsumer
from kafka import TopicPartition
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable



class KConsumer(threading.Thread):

    def __init__(self, Kbroker, Kscival_topic):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.Kbrokers=Kbrokers
        self.Kscival_topic=Kscival_topic
    def stop(self):
        self.stop_event.set()


    def run(self):

        # fetch.max.wait.ms.bytes=100,
        # fetch.min.bytes=1,
        print(Kbrokers)
        consumer = KafkaConsumer(bootstrap_servers=Kbrokers,
                                 auto_offset_reset='latest',
                                 value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                                 consumer_timeout_ms=1000)
        consumer.subscribe([Kscival_topic])
        print("Kbroker:",Kbrokers)
        print("Kscival_topic:",Kscival_topic)
        print("Consumer:",consumer)

        while not self.stop_event.is_set():
            for message in consumer:
                # print(message)
                v_documentSetId=message.value['after']['ds_id']
                mysql_ds_id, mysql_ds_cnt =docSet.my_docset(v_documentSetId)
                print("mysql_data",mysql_ds_id,"    mysql_data",mysql_ds_cnt)

                #graph_ds_id, graph_ds_cnt =grph.find_docset(v_documentSetId,mysql_ds_id,mysql_ds_cnt)
                # print("graph_data",graph_ds_id,"    graph_data",graph_ds_cnt)
                #self.check_counts(mysql_ds_id, mysql_ds_cnt,graph_ds_id, graph_ds_cnt)
                #print("graph_data",graph_ds_id,"    graph_data",graph_ds_cnt)

                if self.stop_event.is_set():
                    break
        consumer.close()

if __name__ == "__main__":
    bolt_url = "neo4j+ssc://127.0.0.1:7687"
    user = "neo4j"
    password = "initial_value"
    #grph = GraphSearch(bolt_url, user, password)
    # GraphSearch.enable_log(logging.INFO , sys.stdout)




    # Kbrokers='b-1.scival-cert-msk.07ps4e.c9.kafka.us-east-1.amazonaws.com:9092'
    # Kbrokers='b-5.scival-prod-msk.3bmz9b.c24.kafka.us-east-1.amazonaws.com:9092,b-1.scival-prod-msk.3bmz9b.c24.kafka.us-east-1.amazonaws.com:9092,b-6.scival-prod-msk.3bmz9b.c24.kafka.us-east-1.amazonaws.com:9092'

    Kbrokers='b-1.scival-cert-msk.07ps4e.c9.kafka.us-east-1.amazonaws.com:9092,b-3.scival-cert-msk.07ps4e.c9.kafka.us-east-1.amazonaws.com:9092,b-6.scival-cert-msk.07ps4e.c9.kafka.us-east-1.amazonaws.com:9092'


    Kscival_topic='prod_document_set'
    Ktopic_part=0
    kaflag=KConsumer( Kbrokers, Kscival_topic)
    kaflag.run()

