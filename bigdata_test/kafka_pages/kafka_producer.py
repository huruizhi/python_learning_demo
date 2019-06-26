#!/usr/bin/python
# coding=utf-8
# 作者：李发富
# 邮箱：fafu_li@live.com & 348926676@qq.com
# 时间：2018.07.05

import sys
# reload(sys)
# sys.setdefaultencoding('utf-8')
try:
    # linux下使用confluent_kafka模块，速度更快，windows上可能没有此模块或者安装非常麻烦
    from confluent_kafka import Producer

    class KafkaP:
        """
        生产模块：根据不同的key，区分消息
        """

        def __init__(self, bootstrap_servers, compression_type='gzip'):
            self.bootstrap_servers = bootstrap_servers
            self.compression_type = compression_type
            self.message_max_bytes = 52428800   # 不要大于kafka服务设置
            if self.compression_type is None:
                self.producer = Producer(
                                        {
                                            'bootstrap.servers': self.bootstrap_servers,
                                            'message.max.bytes': self.message_max_bytes
                                        }
                                        )
            else:
                self.producer = Producer(
                                        {
                                            'bootstrap.servers': self.bootstrap_servers,
                                            'message.max.bytes': self.message_max_bytes,
                                            'compression.type': self.compression_type
                                        }
                                        )

        def send_data(self, message, topic):
            # self.producer.send(topic=topic, key=key, value=message)
            self.producer.produce(topic, message.encode('utf-8'))
            # self.producer.flush()

        def reconnection_producer(self):
            if self.compression_type is None:
                self.producer = Producer(
                                        {
                                            'bootstrap.servers': self.bootstrap_servers,
                                            'message.max.bytes': self.message_max_bytes
                                        }
                                        )
            else:
                self.producer = Producer(
                                        {
                                            'bootstrap.servers': self.bootstrap_servers,
                                            'message.max.bytes': self.message_max_bytes,
                                            'compression.type': self.compression_type
                                        }
                                        )

        def close_producer(self):
            self.producer.flush()
            # self.producer.stop()
            # self.client.close()

except BaseException:
    # from kafka import SimpleProducer, SimpleClient
    #
    # class KafkaP:
    #     """
    #     生产模块：根据不同的key，区分消息
    #     """
    #
    #     def __init__(self, bootstrap_servers):
    #         self.bootstrap_servers = bootstrap_servers
    #         self.retries = 3
    #         self.ack = 0
    #         self.async = True
    #         self.client = SimpleClient(hosts=self.bootstrap_servers)
    #         self.producer = SimpleProducer(client=self.client,
    #                                        async_send=self.async,
    #                                        req_acks=SimpleProducer.ACK_NOT_REQUIRED
    #                                        )
    #
    #     def send_data(self, message, topic):
    #         # self.producer.send(topic=topic, key=key, value=message)
    #         self.producer.send_messages(topic, message.encode('utf-8'))
    #         # self.producer.send_messages(topic, bytes(message))
    #         # self.producer.flush()
    #
    #     def reconnection_producer(self):
    #         self.client = SimpleClient(hosts=self.bootstrap_servers)
    #         self.producer = SimpleProducer(client=self.client,
    #                                        async_send=self.async,
    #                                        req_acks=SimpleProducer.ACK_NOT_REQUIRED
    #                                        )
    #
    #     def close_producer(self):
    #         # self.producer.flush()
    #         self.producer.stop()
    #         self.client.close()

    # windows下使用kafka-python模块的KafkaProducer，速度虽然没有SimpleProducer快，但是产生消息的timestamp字段正常
    # 此timestamp字段在 sync_slave 当加入新的消费者组时应该会使用到(安装timestamp查找offset)
    from kafka import KafkaProducer

    class KafkaP:
        """
        生产模块：根据不同的key，区分消息
        """

        def __init__(self, bootstrap_servers, compression_type='gzip'):
            self.bootstrap_servers = bootstrap_servers
            self.retries = 3
            self.ack = 0
            self.linger_ms = 0
            self.compression_type = compression_type
            if self.compression_type is None:
                self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                              retries=self.retries,
                                              acks=self.ack,
                                              linger_ms=self.linger_ms,
                                              )
            else:
                self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                              retries=self.retries,
                                              acks=self.ack,
                                              linger_ms=self.linger_ms,
                                              compression_type=self.compression_type
                                              )

        def send_data(self, message, topic, key=None):
            self.producer.send(topic=topic, key=key, value=message)
            # print message

        def reconnection_producer(self):
            if self.compression_type is None:
                self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                              retries=self.retries,
                                              acks=self.ack,
                                              linger_ms=self.linger_ms
                                              )
            else:
                self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                              retries=self.retries,
                                              acks=self.ack,
                                              linger_ms=self.linger_ms,
                                              compression_type=self.compression_type
                                              )

        def close_producer(self):
            self.producer.flush()
            self.producer.close()


if __name__ == '__main__':
    BOOTSTRAP_SERVERS = "pycdhnode2:9092,pycdhnode3:9092,pycdhnode4:9092"
    KAFKA_TOPIC = "test"
    PRODUCER = KafkaP(BOOTSTRAP_SERVERS)
    # MESSAGE = "leffss.py_etl_non_monetary_funds_risk_monthly_2_1"
    MESSAGE = 'test'
    for i in range(0, 1):
        PRODUCER.send_data(message=MESSAGE, topic=KAFKA_TOPIC)
    PRODUCER.close_producer()


