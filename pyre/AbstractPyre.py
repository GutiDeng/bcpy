from bcpy.daemon import Daemon

from RedisProtocol import RedisProtocol

class AbstractPyreHandler:
    def __init__(self, sock):
        self._pyre_sock = sock 
        self._pyre_redis_protocol = RedisProtocol()
        self._pyre_cmdmap = {
            'UNIMPLEMENTED': self.UNIMPLEMENTED,
        }
    
    def _pyre_reply(self, *args):
        self._pyre_redis_protocol.take_response(*args)
    
    def handle(self):
        pass
    
    def UNIMPLEMENTED(self, req):
        self._pyre_reply('-', 'ERR command `%s` is not implemented' % req[0])
        

class AbstractPyreServer:
    def __init__(self):
        self._pyre_host = '0.0.0.0'
        self._pyre_port = 0
        self._pyre_handler_class = AbstractPyreHandler
        
    def pyre_serve(self):
        pass
