#!/usr/bin/env python3
# coding: utf-8

import logging
from pykafka import KafkaClient
from pykafka.common import OffsetType



class KafkaConnector(object):
    """docstring for KafkaConnector."""

    def __init__(self, kafka_config):
        super(KafkaConnector, self).__init__()

        self.kafka_config = kafka_config

        if self.kafka_config is None:
            raise Exception("kafka config error: missing config")

        if "hosts" not in self.kafka_config or "zookeeper_connector" not in self.kafka_config:
            raise Exception("kafka config error: cannot find 'hosts' or 'zookeeper_connector'")

    def simple_consumer(self, topic, latest=True):
        hosts = self.kafka_config.get("hosts")

        client = KafkaClient(hosts=hosts)

        if not client.topics[topic.encode()]:
            logging.error("can not find the topic: {0}".format(topic))
            raise ValueError("can not find the topic")

        topic = client.topics[topic.encode()]

        if latest is True:
            offset_type = OffsetType.LATEST
        else:
            offset_type = OffsetType.EARLIEST

        return topic.get_simple_consumer(
            auto_commit_enable=True,
            auto_offset_reset=offset_type,
            reset_offset_on_start=True,
            use_rdkafka=True,
            auto_commit_interval_ms=1,
        )

    def balanced_consumer(self, topic, use_rdkafka, consumer_group="default-consumer-group"):
        hosts = self.kafka_config.get("hosts")
        zookeeper_connector = self.kafka_config.get("zookeeper_connector")

        client = KafkaClient(hosts=hosts)

        if not client.topics[topic.encode()]:
            logging.error("can not find the topic: {0}".format(topic))
            raise ValueError("can not find the topic")

        topic = client.topics[topic.encode()]

        return topic.get_balanced_consumer(
            consumer_group=consumer_group.encode(),
            auto_commit_enable=True,
            zookeeper_connect=zookeeper_connector,
            use_rdkafka=use_rdkafka
        )

    def get_producer(self, topic):
        hosts = self.kafka_config.get("hosts")

        client = KafkaClient(hosts=hosts)

        topic = client.topics[topic.encode()]

        return topic.get_sync_producer()