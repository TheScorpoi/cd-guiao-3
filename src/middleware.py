"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from typing import Any
import pickle
import json
import xml.etree.ElementTree as element_tree
import xml
import selectors
import socket

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
        self.host = 'localhost'
        self.port = 5000
        self.selector = selectors.DefaultSelector()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        self.selector.register(self.socket, selectors.EVENT_READ, self.pull)
        #self.AckMessage(self.protocol, self._type, self._topic)
        #if self.type==MiddlewareType.CONSUMER:
            #subscribe to the topic passed as a command line argument, for example : --type weather or --type /
        #self.subscribe(self.topic)
                
    def push(self, value):
        """Sends data to broker. """
        self.send_message('PUBLISH', value)


    def pull(self) -> (str, Any):
        """Waits for (topic, data) from broker.
        Should BLOCK the consumer!"""
        data = self.socket.recv(1000)
        
        if data:
            method, topic, message = self.decode(data)
            return topic, message
        else:
            return topic, "No data"

    def send_message(self, method, data):
        data = self.encode(data, self._topic, method)
        self.socket.send(data)
        
    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        self.send_message('LIST', "")
        data = self.socket.recv(1000)
        if data:
            for topic in data.decode('utf-8'):
                print(topic)

            # method, topic, message = self.decode(data)
            # return topic, message
            #! OU ASSIM???

    def cancel(self):
        """Cancel subscription."""
        self.send_message('CANCEL', "")

    def subscribe(self, topic):
        self.send_message("SUBSCRIBE" , topic)



class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type=_type)
    
    @classmethod
    def encode(cls, message, topic, method):
        protocol_JSON = {"method": method, 'topic': topic, "message": message}
        protocol_JSON = json.dumps(protocol_JSON)
        protocol_JSON = protocol_JSON.encode('utf-8')
        return protocol_JSON
        
    @classmethod
    def decode(cls, data):
        data = data.decode('utf-8')
        data = json.loads(data)
        method = data['method']
        topic = data['topic']
        message = data['message']
        return method, topic, message
    
class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type=_type)
        
    @classmethod
    def encode(cls, message, topic, method):
        protocol_XML = {"method": method, 'topic': topic, "message": message}
        protocol_XML = ('<?xml version="1.0"?><data method="%(method)s" topic="%(topic)s"><message>%(message)s</message></data>' % protocol_XML)
        protocol_XML = protocol_XML.encode('utf-8')
        return protocol_XML
        
    @classmethod
    def decode(cls, data):
        data = data.decode('utf-8')
        data = element_tree.fromstring(data)
        message_xml = data.attrib
        method = message_xml['method']
        topic = message_xml['topic']
        message = message_xml.find('message').txt
        return method, topic, message

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type=_type)
    
    @classmethod
    def encode(cls, message, topic, method):
        protocol_PICKLE = {"method": method, 'topic': topic, "message": message}
        protocol_PICKLE = pickle.dumps(protocol_PICKLE)
        return protocol_PICKLE

    @classmethod
    def decode(cls, data):
        self.data = pickle.loads(data)
        method = data['method']
        topic = data['topic']
        message = data['message']
        return method, topic, message