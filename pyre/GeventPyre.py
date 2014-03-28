from AbstractPyre import AbstractPyreHandler, AbstractPyreServer

from gevent.server import StreamServer
from gevent import socket
import gevent

class GeventPyreHandler(AbstractPyreHandler):
    def __init__(self, sock):
        AbstractPyreHandler.__init__(self, sock)
        self._pyre_cmdmap.update({
            'PING': self.PING,
            'SELECT': self.SELECT,
            'INFO': self.INFO,
        })
    def PING(self, req):
        self._pyre_reply('+', 'PONG')
    def SELECT(self, req):
        self._pyre_reply('+', 'OK')
    def INFO(self, req):
        self._pyre_reply('$', '# bcpy.pyre.GeventPyreHandler\r\nauthor:Guti\r\n')
    
    def handle(self):
        p = self._pyre_redis_protocol
        while True:
            s = self._pyre_sock.recv(512)
            if not s:
                break
            
            p.take_request(s)
            
            for req in p.pop_requests():
                cmd = req[0].upper()
                if cmd not in self._pyre_cmdmap:
                    cmd = 'UNIMPLEMENTED'
                self._pyre_cmdmap[cmd](req)
            
            for resp in p.pop_responses():
                self._pyre_sock.send(resp)
        self._pyre_sock.shutdown(socket.SHUT_WR)
        self._pyre_sock.close()
        

class GeventPyreServer(AbstractPyreServer):
    def __init__(self):
        AbstractPyreServer.__init__(self)
        self._pyre_handler_class = GeventPyreHandler
        
    def pyre_serve(self):
        server = StreamServer((self._pyre_host, self._pyre_port), self._pyre_handle)
        server.serve_forever()
    
    def _pyre_handle(self, sock, addr):
        h = self._pyre_handler_class(sock)
        h.handle()

if __name__ == '__main__':
    s = GeventPyreServer()
    s._pyre_port = 19700
    s.pyre_serve()

