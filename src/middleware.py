"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
import json
import pickle
import xml

class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self._topic = topic
        self._type = _type
        self.queue = queue._type(f"{topic}", self._type)
        
                
    def push(self, value):
        """Sends data to broker. """



    def pull(self) -> (str, tuple):
        """Receives (topic, data) from broker.

        Should BLOCK the consumer!"""


    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""


    def cancel(self):
        """Cancel subscription."""


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type=_type)
    
    @classmethod
    def encodeJSON(cls, message, topic, method):
        protocol_JSON = {"method": method, 'topic': topic, "message": message}
        protocol_JSON = json.dumps(protocol_JSON)
        protocol_JSON = protocol_JSON.encode('utf-8')
        return protocol_JSON
        
    @classmethod
    def decodeJSON(cls, data):
        self.data = data.decode('utf-8')
        self.data = json.loads(data)
        method = data['method']
        topic = data['topic']
        message = data['message']
        return method, topic, message
    
class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type=_type)
        
    @classmethod
    def encodeXML(cls, message, topic, method):
        protocol_XML = {"method": method, 'topic': topic, "message": message}
        protocol_XML = {'<?xml version = "1.0" encoding = "UTF-8" standalone = "no" ?>'}
        protocol_XML = xml.encode('utf-8')
        return protocol_XML
        
    @classmethod
    def decodeXML(cls, data):
        return data 

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type=_type)
    
    @classmethod
    def encodePICKLE(cls, message, topic, method):
        protocol_PICKLE = {"method": method, 'topic': topic, "message": message}
        protocol_PICKLE = pickle.dumps(protocol_PICKLE)
        return protocol_PICKLE

    @classmethod
    def decodePICKLE(cls, data):
        self.data = pickle.loads(data)
        method = data['method']
        topic = data['topic']
        message = data['message']
        return method, topic, message