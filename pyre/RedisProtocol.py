from itertools import imap

SYM_ASTERISK = '*'
SYM_DOLLAR = '$'
SYM_CRLF = '\r\n'
SYM_LF = '\n'

class RedisProtocol:
    def __init__(self):
        self._requests = []
        self._responses = []
        
        self._request_expecting_line_type = '*'
        self._request_expecting_arg_num = 0
        self._request_args = []
        self._request_last_line_remains = ''
    
    def take_request(self, s):
        #print 'take_request', repr(s)
        if self._request_last_line_remains:
            s = self._request_last_line_remains + s
        start = stop = 0
        while True:
            stop = s.find('\r\n', start)
            if stop > -1:
                self._take_request_line(s[start:stop])
                start = stop + 2
            else:
                if start < len(s):
                    self._request_last_line_remains = s[start:]
                else:
                    self._request_last_line_remains = ''
                break
    
    def _take_request_line(self, line):
        #print '_take_request_line', repr(line)
        if self._request_expecting_line_type == '*':
            assert line[0] == '*'
            self._request_expecting_arg_num = int(line[1:])
            self._request_expecting_line_type = '$'
        elif self._request_expecting_line_type == '$':
            assert line[0] == '$'
            self._request_expecting_arg_length = int(line[1:])
            self._request_expecting_line_type = ''
        elif self._request_expecting_line_type == '':
            assert len(line) == self._request_expecting_arg_length
            self._request_args.append(line)
            self._request_expecting_arg_num -= 1
            if self._request_expecting_arg_num == 0:
                self._request_expecting_line_type = '*'
                self._requests.append( tuple(self._request_args) )
                del self._request_args[:]
                self._request_expecting_line_type = '*'
            else:
                self._request_expecting_line_type = '$'
        
        
    def take_response(self, *args):
        # print self._repr_response(*args)
        self._responses.append( self._repr_response(*args) )
        return
    def _repr_response(self, *args):
        rtype = args[0]
        if rtype == '*':
            return '*' + repr(len(args)-1) + SYM_CRLF + \
                    ''.join(['$' + repr(len(encoded)) + SYM_CRLF + \
                        encoded + SYM_CRLF \
                        for encoded in imap(self._repr, args[1:])
                    ])
        elif rtype == '$':
            if args[1] is None:
                return '$-1' + SYM_CRLF
            encoded = self._repr(args[1])
            return '$' + repr(len(encoded)) + SYM_CRLF + encoded + SYM_CRLF
        else:
            return args[0] + self._repr(args[1]) + SYM_CRLF
    def _repr(self, value):
        '''return a bytestring representation of the value'''
        if isinstance(value, bytes):
            return value
        if isinstance(value, float):
            value = repr(value)
        if not isinstance(value, basestring):
            value = str(value)
        if isinstance(value, unicode):
            value = value.encode('UTF-8', 'strict')
        return value
    
    def pop_requests(self):
        for i in xrange(len(self._requests)):
            req = self._requests.pop()
            yield req
    
    def pop_responses(self):
        for i in xrange(len(self._responses)):
            resp = self._responses.pop()
            yield resp


