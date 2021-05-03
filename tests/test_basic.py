"""Test simple consumer/producer interaction."""
import json
import pickle
import random
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from src.broker import Broker
from src.clients import Consumer, Producer
from src.middleware import JSONQueue, PickleQueue


def gen():
    while True:
        yield random.randint(0, 100)


@pytest.fixture(scope="session", autouse=True)
def broker():
    broker = Broker()

    thread = threading.Thread(target=broker.run, daemon=True)
    thread.start()
    time.sleep(1)
    return broker


@pytest.fixture
def consumer_JSON():
    consumer = Consumer("/temp", JSONQueue)

    thread = threading.Thread(target=consumer.run, daemon=True)
    thread.start()
    return consumer


@pytest.fixture
def consumer_Pickle():
    consumer = Consumer("/temp", PickleQueue)

    thread = threading.Thread(target=consumer.run, daemon=True)
    thread.start()
    return consumer


@pytest.fixture
def producer_JSON():

    producer = Producer("/temp", gen, PickleQueue)

    producer.run(1)
    return producer


def test_simple_producer_consumer(consumer_JSON):

    producer = Producer("/temp", gen, JSONQueue)

    with patch("json.dumps", MagicMock(side_effect=json.dumps)) as json_dump:
        with patch("pickle.dumps", MagicMock(side_effect=pickle.dumps)) as pickle_dump:

            producer.run(10)
            assert pickle_dump.call_count == 0
            assert json_dump.call_count >= 10  # at least 10 JSON messages

    time.sleep(0.1)  # wait for messages to propagate through the broker to the clients

    assert consumer_JSON.received == producer.produced


def test_multiple_consumers(consumer_JSON, consumer_Pickle):

    prev = list(consumer_JSON.received)  # consumer gets previously stored element

    producer = Producer("/temp", gen, PickleQueue)

    producer.run(9)  # iterate only 9 times, consumer iterates 9 + 1 historic
    time.sleep(0.1)  # wait for messages to propagate through the broker to the clients

    assert consumer_JSON.received == prev + producer.produced
    assert consumer_Pickle.received == consumer_JSON.received


def test_broker(producer_JSON, broker):
    time.sleep(0.1)  # wait for messages to propagate through the broker to the clients

    assert broker.list_topics() == ["/temp"]

    assert broker.get_topic("/temp") == producer_JSON.produced[-1]
