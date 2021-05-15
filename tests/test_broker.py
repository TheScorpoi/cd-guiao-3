"""Test simple consumer/producer interaction."""
import json
import pickle
import random
from src.protocol import Serializer
import string
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from src.broker import Broker, Serializer

@pytest.fixture(scope="session", autouse=True)
def broker():
    broker = Broker()
    return broker

def test_subscriptions(broker):
    fake_subscriber1 = MagicMock()
    fake_subscriber2 = MagicMock()

    broker.subscribe("/t1", fake_subscriber1, Serializer.JSON)
    broker.subscribe("/t2", fake_subscriber1, Serializer.JSON)
    broker.subscribe("/t2", fake_subscriber2, Serializer.PICKLE)

    assert broker.list_subscriptions("/t1") == [(fake_subscriber1, Serializer.JSON)]

    assert len(broker.list_subscriptions("/t2")) == 2
    assert (fake_subscriber1, Serializer.JSON) in broker.list_subscriptions("/t2") 
    assert (fake_subscriber2, Serializer.PICKLE) in broker.list_subscriptions("/t2")

    broker.unsubscribe("/t2", fake_subscriber1)
    assert broker.list_subscriptions("/t2") == [(fake_subscriber2, Serializer.PICKLE)]

def test_topics(broker):
    broker.put_topic("/t1", 1000)

    assert broker.get_topic("/t1") == 1000

    assert broker.get_topic("/t2") == None

    broker.put_topic("/t2", "abc")

    assert broker.get_topic("/t2") == "abc"

    assert len(broker.list_topics()) == 2
    assert "/t1" in broker.list_topics()
    assert "/t2" in broker.list_topics()