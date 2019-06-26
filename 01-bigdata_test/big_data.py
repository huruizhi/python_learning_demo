import sqlalchemy as sa
import time, datetime
from elasticsearch import Elasticsearch
from kafka_lib import KafkaProducer
from kafka_lib import KafkaConsumer
from kafka_lib.errors import KafkaError
import json
import time


def log(str):
    t = time.strftime(r"%Y-%m-%d_%H-%M-%S", time.localtime())
    print("[%s]%s" % (t, str))

class Kylin:

    @staticmethod
    def kylin_query(date_time):
        tm = time.strptime(date_time, '%Y-%m-%d %H:%M:%S')
        timeStamp = int(time.mktime(tm))*1000
        kylin_engine = sa.create_engine('kylin://ADMIN:KYLIN@125.64.43.160:7070/fundnews?version=v2')
        sql_str = '''
        select ALG_KEYWORD,count(ALG_NEWS_NUM) as ALG_NEWS_NUM from DATAMART_HOTSPOT_WORD_ANALY
        where ALG_TIME>{timeStamp}
        group by ALG_KEYWORD order by ALG_NEWS_NUM desc  limit 10
        '''.format(timeStamp=timeStamp)
        results = kylin_engine.execute(sql_str)
        for i in results:
            print(i)


class Es:
    def __init__(self):
        link_info = {"host": '221.236.16.197', "port": 9200}
        self.es = Elasticsearch(hosts=[link_info, ])

    def es_query(self):
        pass


class KafkaC:
    """
        消费模块: 通过不同groupid消费topic里面的消息
    """
    def __init__(self, kafka_host, kafka_port, kafka_topic, group_id):
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.kafka_topic = kafka_topic
        self.group_id = group_id

        self.consumer = KafkaConsumer(self.kafka_topic, group_id=self.group_id,
                                      bootstrap_servers='{kafka_host}:{kafka_port}'.format(
                                          kafka_host=self.kafka_host,
                                          kafka_port=self.kafka_port)
                                      )

    def consume_data(self):
        try:
            for message in self.consumer:
                yield message
        except KeyboardInterrupt as e:
            print(e)


class KafkaP:
    """
    生产模块：根据不同的key，区分消息
    """

    def __init__(self, kafka_host, kafka_port, kafka_topic, key):
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.kafka_topic = kafka_topic
        self.key = key
        self.producer = KafkaProducer(bootstrap_servers='{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafka_host,
            kafka_port=self.kafka_port)
        )

    def send_json_data(self, params):
        try:
            parmas_message = json.dumps(params)
            producer = self.producer
            producer.send(self.kafka_topic, key=self.key, value=parmas_message.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print(e)


if __name__ == '__main__':
    # KAFAKA_HOST = "172.16.80.112"
    # KAFAKA_PORT = 6667
    # KAFAKA_TOPIC = "News-Recommendation-Spider-Data"
    # group = 'pycf'
    # consumer = KafkaC(KAFAKA_HOST, KAFAKA_PORT, KAFAKA_TOPIC, group)
    # message = consumer.consume_data()
    # for msg in message:
    #     value = msg.value
    #     print(value.decode('utf8'))
    Es()




