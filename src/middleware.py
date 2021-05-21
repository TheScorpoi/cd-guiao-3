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
        self.topic = topic
        self._type = _type
        self.host = 'localhost'
        self.port = 5003 
        self.selector = selectors.DefaultSelector()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        self.selector.register(self.socket, selectors.EVENT_READ, self.pull)
        ack_msg = json.dumps({"method": "ACK", "Serializer": str(self.__class__.__name__)}).encode('utf-8')
        header = len(ack_msg).to_bytes(2, "big")   
        self.socket.send(header + ack_msg)

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
        
        header = self.socket.recv(2)                        
        header = int.from_bytes(header, "big")  
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
        header = len(data).to_bytes(2, "big")  
        self.socket.send(header + data)       

class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):  
        super().__init__(topic, _type)
        
    def decode(self, data):
        data=data.decode('utf-8')
        msg=json.loads(data)
        op=msg['method']
        topic=msg['topic']
        msg=msg['msg']
        return op,topic,msg  
    
    def encode(self, method, topic,msg):
        init={'method':method,'topic':topic,'msg':msg}
        init=json.dumps(init)
        init=init.encode('utf-8')
        return init   



class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        
    def encode(self,method,topic,msg):
        init={'method':method,'topic':topic,'msg':msg}
        init=('<?xml version="1.0"?><data method="%(method)s" topic="%(topic)s"><msg>%(msg)s</msg></data>' % init)
        init=init.encode('utf-8')
        return init
    
    def decode(self,data):
        init=data.decode('utf-8')
        init=XM.fromstring(init)
        init2=init.attrib
        op=init2['method']
        topic=init2['topic']
        msg=init.find('msg').text
        return op,topic,msg


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        
    def encode(self,method, topic,msg):
        init={'method':method,'topic':topic,'msg':msg}
        init=pickle.dumps(init)
        return init
    
    def decode(self,data):
        msg=pickle.loads(data)
        op=msg['method']
        topic=msg['topic']
        msg=msg['msg']
        return op,topic,msg

