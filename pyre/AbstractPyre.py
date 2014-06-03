from RedisProtocol import RedisProtocol

class AbstractPyreHandler:
    
    def pyre_handle(self):
        pass
    
    def pyre_initialise(self, sock, addr, server):
        self.pyre_sock = sock 
        self.pyre_server = server
        
        self.pyre_redis_protocol = RedisProtocol()
        
        self.pyre_command_handlers = {}
        
        self.pyre_register_commands()
    
    def pyre_register_commands(self):
        self.pyre_register_command(self.UNIMPLEMENTED)
        
    def pyre_register_command(self, handler, name=None):
        name = name or handler.__name__
        self.pyre_command_handlers[name]= handler
    
    def pyre_get_command_handler(self, cmd):
        return self.pyre_command_handlers.get(cmd.upper(), 
                self.pyre_command_handlers['UNIMPLEMENTED'])
        
    def pyre_reply(self, *args):
        self.pyre_redis_protocol.take_response(*args)
    
    def UNIMPLEMENTED(self, req):
        self.pyre_reply('-', 'ERR command `%s` is not implemented' % req[0])
        

class AbstractPyreServer:
    pyre_host = '0.0.0.0'
    pyre_port = 0
    pyre_handler_class = AbstractPyreHandler
    
    def pyre_serve(self):
        pass
    
    def pyre_handle(self, sock, addr):
        handler = self.pyre_handler_class()
        handler.pyre_initialise(sock, addr, self)
        handler.pyre_handle()
