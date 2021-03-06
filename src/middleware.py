"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
from queue import LifoQueue, Empty
from typing import Any
from broker import Broker
import socket
import selectors
import json
import pickle
import xml.etree.ElementTree as element_tree
import xml


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.host = 'localhost'
        self.port = 5000  
        self.topic = topic
        self._type = _type
        self.selector = selectors.DefaultSelector()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        ack_msg = json.dumps({"method": "ACK", "Serializer": str(self.__class__.__name__)}).encode('utf-8')
        header = len(ack_msg).to_bytes(3, "little")   
        self.socket.send(header + ack_msg)
        self.selector.register(self.socket, selectors.EVENT_READ, self.pull)

        if self._type==MiddlewareType.CONSUMER:
            self.subscribe(topic)

    def subscribe(self, topic):
        self.send_message('SUBSCRIBE' , topic)

    def push(self, value):
        """Sends data to broker. """
        print(value)
        self.send_message('PUBLISH', value)


    def pull(self) -> (str, Any):
        """Waits for (topic, data) from broker.
        Should BLOCK the consumer!"""
        
        header = self.socket.recv(3)                        
        header = int.from_bytes(header, "little")  
        data = self.socket.recv(header)
        
        if data:           
            method, topic, msg = self.decode(data)   
            return topic, msg    

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        self.send_message('LIST', '')

    def cancel(self):
        """Cancel subscription."""
        self.send_message('CANCEL', self.topic)

    def send_message(self, method, message):
        """Sends through a connection a Message object."""
        data = self.encode(method, self.topic, message) 
        header = len(data).to_bytes(3, "little")  
        self.socket.send(header + data)       

class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):  
        super().__init__(topic, _type)
    
    def encode(self, method, topic, message):
        msg_JSON = {'method': method, 'topic': topic, 'msg': message}
        msg_JSON = json.dumps(msg_JSON)
        msg_JSON = msg_JSON.encode('utf-8')
        
        return msg_JSON   
     
    def decode(self, data):
        data = data.decode('utf-8')
        message = json.loads(data)
        method = message['method']
        topic = message['topic']
        message = message['msg']
        
        return method, topic, message  

class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        
    def encode(self,method, topic, msg):
        msg_XML = {'method': method, 'topic': topic, 'msg': msg}
        msg_XML = ('<?xml version="1.0"?><data method="%(method)s" topic="%(topic)s"><msg>%(msg)s</msg></data>' % msg_XML)
        msg_XML = msg_XML.encode('utf-8')
        
        return msg_XML
    
    def decode(self, data):
        data = data.decode('utf-8')
        data = element_tree.fromstring(data)
        data_aux = data.attrib
        method = data_aux['method']
        topic= data_aux['topic']
        message = data.find('msg').text

        return method, topic, message

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        
    def encode(self,method, topic, message):
        msg_PICKLE = {'method': method, 'topic': topic, 'msg': message}
        msg_PICKLE = pickle.dumps(msg_PICKLE)
    
        return msg_PICKLE

    def decode(self,data):
        data = pickle.loads(data)
        method = data['method']
        topic = data['topic']
        message = data['msg']
        
        return method, topic, message

