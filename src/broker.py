"""Message Broker"""
import enum
from typing import Dict, List, Any, Tuple
import selectors
import socket

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

    def accept(self, sock, mask):
        conn, addr = sock.accept()
        print('accepted', conn, 'from', addr)
        conn.setblocking(False)
        self._sel.register(conn, selectors.EVENT_READ, self.read) 
        self._connKeyDic[conn] = [None] 
        
    def read(self,conn, mask):
        try:
            data = CDProto.recv_msg(conn) 
            print('echoing', str(data), 'to', conn)
            
            if (data._command == "join" ):
                channelList = self._connKeyDic[conn]  
                
                if data._channel not in channelList:
                    channelList.append(data._channel)
                
                    if(None in channelList):   
                        channelList.remove(None)
                    
                    self._connKeyDic[conn] = channelList    

            if (data._command == "message"):    
                for k,v in self._connKeyDic.items():    
                    for channel in v:
                        if data._channel == channel:    
                            CDProto.send_msg(k, data)   
        except ConnectionError:
            print('closing', conn)
            self._sel.unregister(conn) 
            self._connKeyDic.pop(conn)  
            conn.close()

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
                   


    def run(self):
        """Run until canceled."""

        while not self.canceled:
            pass
