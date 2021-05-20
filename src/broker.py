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
        self.sel = selectors.DefaultSelector()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self._host, self._port))
        self.socket.listen(100)
        self.sel.register(self.socket, selectors.EVENT_READ, self.accept)
        
        self.subscribedDic = {}
        self.topicsDic = {}
        self.userDic = {} #!topic_userSerializer
        self.topicMsgDic = {}

    def accept(self, sock, mask):
        conn, addr = sock.accept()
        print('accepted', conn, 'from', addr)
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)
        
        data = conn.recv(1000).decode('utf-8')
        data = json.loads(data)

        if data:
            if data['Serializer'] == 'JSONQueue':
                self.subscribedDic[conn] = Serializer.JSON
            elif data['Serializer'] == 'XMLQueue':
                self.subscribedDic[conn] = Serializer.XML
            elif data['Serializer'] == 'PickleQueue':
                self.subscribedDic[conn] = Serializer.PICKLE
        else:
            print('closing ', conn)
            self.sel.unregister(conn)
            conn.close()

    def read(self, conn, mask):
        data = conn.recv(1000)
        if data:
            if conn in self.subscribedDic.keys():
                # first check how to decode the message
                if self.subscribedDic[conn] == Serializer.JSON:
                    method, topic, msg = self.decodeJSON(data)
                elif self.subscribedDic[conn] == Serializer.PICKLE:
                    method, topic, msg = self.decodePICKLE(data)
                elif self.subscribedDic[conn] == Serializer.XML:
                    method, topic, msg = self.decodeXML(data)

                if method == 'PUBLISH':
                    # ler com a funcao de put_topic
                    self.put_topic(topic, msg)

                elif method == 'SUBSCRIBE':
                    # ler com a funcao de subscribe
                    self.subscribe(topic, conn, self.subscribedDic[conn])

                    if topic in self.topicsDic:
                        self.send_msg(conn, 'Ultima_message', topic, self.topicsDic[topic])
                        #message no ultimo agr

                elif method == 'CANCEL':
                    # ler com a funcao de cancelar a sub
                    self.unsubscribe(topic, conn)

                elif method == 'LIST':
                    # ler com a funcao de retornar todas as coisas da lista
                    self.send_msg(conn, 'LIST', topic, self.list_topics())
        else:
            print('closing', conn)
            for i in self.userDic.keys():
                users = self.userDic[i]
                for f in users:
                    if f[0] == conn:
                        self.userDic[i].remove(f)
                        break

            self.sel.unregister(conn)
            conn.close()

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics."""
        list_topics = []
        for i in self.topicMsgDic :
            list_topics.append(i)
        return list_topics

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        if topic in self.topicsDic.keys():
            return self.topicsDic[topic]
        else:
            return None

    def put_topic(self, topic, value):
        """Store in topic the value."""
        #if topic not in self.topicsDic:
        self.topicsDic[topic] = value
        
        if topic not in self.topicMsgDic.keys():
            for topico in self.topicMsgDic.keys():
                if topico in topic:
                    self.topicMsgDic[topico].append(topic)

            self.topicMsgDic[topic] = []
            for topico in self.topicMsgDic.keys():
                if topic in topico:
                    self.topicMsgDic[topic].append(topico)

        for i in self.topicMsgDic[topic]:
            if i in self.userDic.keys():
                if self.userDic[i] != []: 
                    address = self.userDic[i][0][0]
                    self.send_msg(address, "Message" , topic, value)

    def list_subscriptions(self, topic: str) -> List[socket.socket]:
        """Provide list of subscribers to a given topic."""
        for key in self.userDic.keys():
            if key == topic:
                return self.userDic[key]

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if(topic not in self.userDic.keys()):
            self.userDic[topic] = [(address, _format)]
        else:
            self.userDic[topic].append((address, _format))
            
        if topic not in self.topicMsgDic.keys():
            for topico in self.topicMsgDic.keys():
                if topico in topic:
                    self.topicMsgDic[topico].append(topic)

            self.topicMsgDic[topic] = []
            for topico in self.topicMsgDic.keys():
                if topic in topico:
                    self.topicMsgDic[topic].append(topico)        

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        '''for key, value in self.userDic.items():
            if key == topic:
                for i in value:
                    if i[0] == address:
                        self.userDic[topic].remove(i)
                        break'''

        if topic in self.userDic.keys():
            list_users = self.userDic[topic]
            for i in list_users:
                if i[0] == address:
                    self.userDic[topic].remove(i)
                    break

    def send_msg(self, conn, method, topic, msg):
        
        if self.subscribedDic[conn] == Serializer.JSON:
            conn.send(self.encodeJSON(msg, topic, method))
        elif self.subscribedDic[conn] == Serializer.PICKLE:
            conn.send(self.encodePICKLE(msg, topic, method))
        elif self.subscribedDic[conn] == Serializer.XML:
            conn.send(self.encodeXML(msg, topic, method))

    def encodeJSON(self, message, topic, method):
        protocol_JSON = {"method": method, 'topic': topic, "message": message}
        protocol_JSON = json.dumps(protocol_JSON)
        protocol_JSON = protocol_JSON.encode('utf-8')
        return protocol_JSON

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
        protocol_PICKLE = {"method": method,'topic': topic, "message": message}
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
        while not self.canceled:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)