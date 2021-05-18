"""Message Broker"""
import enum
from typing import Dict, List, Any, Tuple
import selectors
import xml.etree.ElementTree as element_tree
import socket
import json
import pickle

class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000
        self._sel = selectors.DefaultSelector()
        self._socket = socket.socket()
        self._socket.bind((self._host, self._port))
        self._socket.listen(100)
        self._sel.register(self._socket, selectors.EVENT_READ, self.accept)
        self.subscribedDic = {}
        self.topicsDic = {}
        self.userDic = {}
        

    def accept(self, sock, mask):
        conn, addr = sock.accept()
        print('accepted', conn, 'from', addr)
        
        data = conn.recv(1000)
        
        if data:
            if data.decode('utf-8') == 'JSONQueue':
                self.userDic[conn] = Serializer.JSON
            elif data.decode('utf-8') == 'XMLQueue':
                self.userDic[conn] = Serializer.XML
            elif data.decode('utf-8') == 'PICKLEQueue':
                self.userDic[conn] = Serializer.PICKLE
        else:
            print('closing ', conn)
            self._sel.unregister(conn)
            conn.close()
        self._sel.register(conn, selectors.EVENT_READ, self.read) 
        
        
    def read(self,conn, mask):
        data=conn.recv(1000)
        if data:
            if conn in self.userDic:
                #first check how to decode the message
                if self.userDic[conn] == Serializer.JSON:
                    method,topic,msg=self.decode(data)
                elif self.userDic[conn] == Serializer.PICKLE:
                    method,topic,msg=self.decode(data)
                elif self.userDic[conn] == Serializer.XML:
                    method,topic,msg=self.decode(data)
                #check the method associated with the message 
                '''
                if method == 'PUBLISH':
                    #ler com a funcao de publish
                elif method == 'SUBSCRIBE':
                    #ler com a funcao de subscribe
                    self.subscribe(topic, conn, self._socket, Serielizer + ".")
                elif method == 'CANCEL':
                    #ler com a funcao de cancelar a sub
                    self.unsubscribe(topic, conn)
                elif method == 'LIST':    
                    #ler com a funcao de retornar todas as coisas da lista
                    self.list_topics()
                '''     

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics."""
        list_topics = []
        for i in self.topicsDic:
            list_topics.append(i)
        return list_topics

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        if topic in self.topicsDic:
            return self.topicsDic[topic]
        else:
            return None
            
    def put_topic(self, topic, value):
        """Store in topic the value."""
        if topic not in self.topicsDic:
            self.topicsDic[topic] = value     

    #def putMSG_on_topic():       

    def list_subscriptions(self, topic: str) -> List[socket.socket]:
        """Provide list of subscribers to a given topic."""
        for key in self.subscribedDic:
            if key == topic:
                return self.subscribedDic[key]
        
    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if(topic not in self.subscribedDic):
            self.subscribedDic[topic] = [(address, _format)]
        else:
            self.subscribedDic[topic].append((address , _format))

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        for key, value in self.subscribedDic.items():
            if key == topic:
                for i in value:
                    if i[0] == address:
                        self.subscribedDic[topic].remove(i)
                        break

    def encodeJSON(self, message, topic, method):
        protocol_JSON = {"method": method, 'topic': topic, "message": message}
        protocol_JSON = json.dumps(protocol_JSON)
        protocol_JSON = protocol_JSON.encode('utf-8')
        return protocol_JSON
        
    @classmethod
    def decodeJSON(self, data):
        data = data.decode('utf-8')
        data = json.loads(data)
        method = data['method']
        topic = data['topic']
        message = data['message']
        return method, topic, message

    def encodeXML(self, message, topic, method):
        protocol_XML = {"method": method, 'topic': topic, "message": message}
        protocol_XML = ('<?xml version="1.0"?><data method="%(method)s" topic="%(topic)s"><message>%(message)s</message></data>' % protocol_XML)
        protocol_XML = protocol_XML.encode('utf-8')
        return protocol_XML
        
    def decodeXML(self, data):
        data = data.decode('utf-8')
        data = element_tree.fromstring(data)
        message_xml = data.attrib
        method = message_xml['method']
        topic = message_xml['topic']
        message = message_xml.find('message').txt
        return method, topic, message

    def encodePICKLE(self, message, topic, method):
        protocol_PICKLE = {"method": method, 'topic': topic, "message": message}
        protocol_PICKLE = pickle.dumps(protocol_PICKLE)
        return protocol_PICKLE

    def decodePICKLE(self, data):
        self.data = pickle.loads(data)
        method = data['method']
        topic = data['topic']
        message = data['message']
        return method, topic, message
                   
    def run(self):
        """Run until canceled."""

        while True:
            events = self._sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
