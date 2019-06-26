from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from kafka import TopicPartition
import json
import datetime


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
                                          kafka_port=self.kafka_port),
                                      # auto_offset_reset='latest',
                                      auto_offset_reset='earliest',
                                      enable_auto_commit=True)
        # ps = [TopicPartition(self.kafka_topic, p) for p in self.consumer.partitions_for_topic(self.kafka_topic)]
        # self.consumer.assign(ps)
        #for partition in ps:
        #    self.consumer.seek(partition, 0)

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

    def __init__(self, kafka_host, kafka_port, kafka_topic, key=None):
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.kafka_topic = kafka_topic
        self.key = key
        self.producer = KafkaProducer(bootstrap_servers=['{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafka_host,
            kafka_port=self.kafka_port), ]
        )

    def send_json_data(self, params):
        try:
            #parmas_message = json.dumps(params)
            producer = self.producer
            producer.send(self.kafka_topic, key=self.key, value=params)
            producer.flush()
        except KafkaError as e:
            print(e)


if __name__ == '__main__':
    """
    从远程电信机房服务器news-data 读取kafka消息对列中的内容
    写入到本地kafka队列
    """
    topic = 'sample'

    consumer_inner = KafkaC("172.16.10.214", 9092, topic, 'log')

    # producer = KafkaP("172.16.10.246", 9092, topic)
    # data = '{"id": "c57efd7b4f8b237690b4c37f624efa7b","url": "http://finance.sina.com.cn/world/gjcj/2018-07-06/doc-ihexfcvk3564898.shtml","content": "123"}'
    # print(data)
    # data_1 = data.encode('utf-8')
    # producer.send_json_data(data_1)

    from elasticsearch5 import Elasticsearch

    message = consumer_inner.consume_data()

    es = Elasticsearch(hosts='elasticsearch-logging.logging.svc.cluster.local')

    for msg in message:
        offset = msg.offset
        print(offset)
        value = msg.value
        value_dic = json.loads(value)
        date_today = datetime.datetime.now().strftime('%Y-%m-%d')
        timestrap = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f+08:00')
        value_dic['timestrap'] = timestrap
        if 'profile' in value_dic:
            index = "java-log-{env}-{date}".format(env=value_dic['profile'].lower(), date=date_today)
            try:
                es.index(index=index, doc_type='javalog', body=value_dic)
            except Exception as e:
                print(value_dic)


