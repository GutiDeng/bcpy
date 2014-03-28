from AbstractPyre import AbstractPyreHandler, AbstractPyreServer

from threading import Thread
import socket

class ThreadingPyreHandler(AbstractPyreHandler, Thread):
    def __init__(self, sock):
        Thread.__init__(self)
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
        self._pyre_reply('$', '# bcpy.pyre.ThreadingPyreHandler\r\nauthor:Guti\r\n')
    
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
    
    def run(self):
        self.handle()
        

class ThreadingPyreServer(AbstractPyreServer):
    def __init__(self):
        AbstractPyreServer.__init__(self)
        self._pyre_handler_class = ThreadingPyreHandler
        
    def pyre_serve(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('localhost', self._pyre_port))
        s.listen(64)
        self.threads = []
        while True:
            conn, addr = s.accept()
            handler = self._pyre_handler_class(conn)
            handler.start()

if __name__ == '__main__':
    s = ThreadingPyreServer()
    s._pyre_port = 19700
    s.pyre_serve()

