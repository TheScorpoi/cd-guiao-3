"""Message Broker"""
from typing import Dict, List, Any, Tuple
from typing import Dict, List, Any, Tuple
import enum
import socket
import selectors
import json
import pickle
import xml
import xml.etree.ElementTree as XM

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
        self._port = 5001
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((self._host,self._port))
        self.socket.listen(100)
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.socket, selectors.EVENT_READ, self.accept)
        #!init dictonaries fazer comentarios sobre key/value em cada um
        
        self.serializer_of_userDic = {}
        self.topics_by_userDic = {}  
        self.messages_of_topicsDic = {}
        self.subtopics_of_topicDic = {}
        
    def accept(self, sock, mask):
        """ """
        conn, addr = sock.accept()                                  
        print('accepted', conn, 'from', addr)
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)

        header_aux = conn.recv(3)                        
        header = int.from_bytes(header_aux, "little")  
        data = conn.recv(header).decode('UTF-8')
        
        if data:
            if json.loads(data)["Serializer"] == 'JSONQueue':
                self.serializer_of_userDic[conn] = Serializer.JSON
            elif json.loads(data)["Serializer"] == 'PickleQueue':
                self.serializer_of_userDic[conn] = Serializer.PICKLE
            elif json.loads(data)["Serializer"] == 'XMLQueue':
                self.serializer_of_userDic[conn] = Serializer.XML
        else:    
            print('closing', conn)                          
            self.sel.unregister(conn)                       
            conn.close() 
            

    def read(self,conn, mask):
        """ """
        header = conn.recv(3)                        
        header = int.from_bytes(header, "little")  
        data = conn.recv(header)

        if data:
            if conn in self.serializer_of_userDic.keys():
                if self.serializer_of_userDic[conn] == Serializer.JSON:
                    method, topic, message = self.decodeJSON(data)
                elif self.serializer_of_userDic[conn] == Serializer.XML:
                    method, topic, message = self.decodeXML(data)
                elif self.serializer_of_userDic[conn] == Serializer.PICKLE:
                    method, topic, message = self.decodePICKLE(data)

                if method == 'PUBLISH':
                    self.put_topic(topic, message)
                elif method == 'SUBSCRIBE':
                    self.subscribe(topic,conn, self.serializer_of_userDic[conn])
                    if topic in self.messages_of_topicsDic:
                        self.send_message(conn, 'LAST_MESSAGE', topic, self.messages_of_topicsDic[topic])
                elif method == 'CANCEL':
                    self.unsubscribe(topic,conn)
                elif method == 'LIST':
                    self.send_message(conn,'LIST_TOPICS_REP',topic, self.list_topics())
            else:
                print('closing', conn)
                for i in self.topics_by_userDic.keys():
                    list_users = self.topics_by_userDic[i]
                    for f in list_users:
                        if f[0] == conn:
                            self.topics_by_userDic[i].remove(f)
                            break

                self.sel.unregister(conn)
                conn.close()

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics."""
        list_topics = []
        for i in self.subtopics_of_topicDic.keys():
            list_topics.append(i)
        return list_topics


    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        if topic in self.messages_of_topicsDic.keys():
            return self.messages_of_topicsDic[topic]
        else:
            return None

    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.messages_of_topicsDic[topic] = value

        if topic not in self.subtopics_of_topicDic.keys():
            for topico in self.subtopics_of_topicDic.keys():
                if topico in topic:
                    self.subtopics_of_topicDic[topico].append(topic)

            self.subtopics_of_topicDic[topic] = []
            for topico in self.subtopics_of_topicDic.keys():
                if topic in topico:
                    self.subtopics_of_topicDic[topic].append(topico)

        for i in self.subtopics_of_topicDic[topic]:
            if i in self.topics_by_userDic.keys():
                if self.topics_by_userDic[i] != []: #
                    address = self.topics_by_userDic[i][0][0]
                    self.send_message(address, 'MESSAGE', topic, value)


    def list_subscriptions(self, topic: str) -> List[socket.socket]:
        """Provide list of subscribers to a given topic."""
        for i in self.topics_by_userDic.keys():
            print(self.topics_by_userDic[i])
            if i==topic:
                return self.topics_by_userDic[i]

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if topic not in self.topics_by_userDic.keys():
            self.topics_by_userDic[topic] = [((address, _format))]
        else:
            self.topics_by_userDic[topic].append((address, _format))
   
        if topic not in self.subtopics_of_topicDic.keys():
            for topico in self.subtopics_of_topicDic.keys():
                if topico in topic:
                    self.subtopics_of_topicDic[topico].append(topic)

            self.subtopics_of_topicDic[topic] = []
            for topico in self.subtopics_of_topicDic.keys():
                if topic in topico:
                    self.subtopics_of_topicDic[topic].append(topico)

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        if topic in self.topics_by_userDic.keys():
            list_users = self.topics_by_userDic[topic]
            for i in list_users:
                if i[0] == address:
                    self.topics_by_userDic[topic].remove(i)
                    break

    def send_message(self,conn, method, topic, message):
        """"""
        if self.serializer_of_userDic[conn] == Serializer.JSON:
            message = self.encodeJSON(method, topic, message) 
        elif self.serializer_of_userDic[conn] == Serializer.PICKLE:
            message = self.encodePICKLE(method, topic, message) 
        elif self.serializer_of_userDic[conn] == Serializer.XML:
            message = self.encodeXML(method, topic, message) 
        
        header = len(message).to_bytes(3, "little")   
        conn.send(header + message)

    def decodeJSON(self, data):
        data=data.decode('utf-8')
        msg=json.loads(data)
        op=msg['method']
        topic=msg['topic']
        msg=msg['msg']
        return op,topic,msg 

    def encodeJSON(self, method, topic,msg):
        init={'method':method,'topic':topic,'msg':msg}
        init=json.dumps(init)
        init=init.encode('utf-8')
        return init

    def encodeXML(self,method,topic,msg):
        init={'method':method,'topic':topic,'msg':msg}
        init=('<?xml version="1.0"?><data method="%(method)s" topic="%(topic)s"><msg>%(msg)s</msg></data>' % init)
        init=init.encode('utf-8')
        return init

    def decodeXML(self,data):
        init=data.decode('utf-8')
        init=XM.fromstring(init)
        init2=init.attrib
        op=init2['method']
        topic=init2['topic']
        msg=init.find('msg').text
        return op,topic,msg

    def encodePICKLE(self,method, topic,msg):
        init={'method':method,'topic':topic,'msg':msg}
        init=pickle.dumps(init)
        return init

    def decodePICKLE(self,data):
        msg=pickle.loads(data)
        op=msg['method']
        topic=msg['topic']
        msg=msg['msg']
        return op,topic,msg

    def run(self):
        """Run until canceled."""
        while not self.canceled:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)