from AbstractPyre import AbstractPyreHandler, AbstractPyreServer

from gevent.server import StreamServer
from gevent import socket
import gevent

class GeventPyreHandler(AbstractPyreHandler):
    
    def pyre_handle(self):
        p = self.pyre_redis_protocol
        while True:
            s = self.pyre_sock.recv(512)
            if not s:
                break
            
            p.take_request(s)
            
            for req in p.pop_requests():
                handler = self.pyre_get_command_handler(req[0])
                handler(req)
            
            for resp in p.pop_responses():
                self.pyre_sock.send(resp)
        
        self.pyre_sock.shutdown(socket.SHUT_WR)
        self.pyre_sock.close()
    
    def pyre_register_commands(self):
        AbstractPyreHandler.pyre_register_commands(self)
        self.pyre_register_command(self.PING)
        self.pyre_register_command(self.SELECT)
        self.pyre_register_command(self.INFO)
        
    def PING(self, req):
        self.pyre_reply('+', 'PONG')
    def SELECT(self, req):
        self.pyre_reply('+', 'OK')
    def INFO(self, req):
        self.pyre_reply('$', '# Server\r\nauthor:Guti\r\n')
    

class GeventPyreServer(AbstractPyreServer):
    pyre_handler_class = GeventPyreHandler
        
    def pyre_serve(self):
        server = StreamServer(
            (self.pyre_host, self.pyre_port),
            self.pyre_handle
        )
        server.serve_forever()
    
if __name__ == '__main__':
    s = GeventPyreServer()
    s.pyre_port = 19700
    s.pyre_serve()

