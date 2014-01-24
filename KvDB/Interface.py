''' This file defines the interface of an KvDB object.

Just like the `DB API 2.0' (http://www.python.org/dev/peps/pep-0249/) defined
an interface to talk with RDBMS, this interface introduce an approach to work 
with Key-Value databases.

Saying that it is about Key-Value databases, the emphasis is not on the 
`database', but on the notion of `Key-Value'.

Traditionally, a Key-Value database is either referring to a `String-String' 
style (Memcached), or an advanced `String-RichStructure' way (Redis).

Both the simplicity of the former and the versatility of the latter sound 
great, and certainly they have brought wonderful memories to us developers.

Here, I am presenting another style, that the value of a key consists of 
one Dictionary and one List simutaniously and automatically.

Mapping and sequence are both typical data structures.The well known JSON 
format can represent nearly everything using these two structures . 

'''

class Interface:
    def __init__(self, connstr, namespace, *args, **kwargs):
        ''' The connstr should be parsed and saved. However an immediate connect
        
        is not required. 
        
        The connstr is in the format of 'host=x port=y ...' .
        '''
        pass
    
    
    ### Keys 
    
    def exists(self, k, pt=3):
        ''' Determines if the given key exists in the namespace.
        
        pt: {1: dict, 2: list, 3: dict or list}
        ''' 
        pass
        # return Boolean
    
    def delete(self, k, pt=3):
        ''' Deletes the given key.
        
        pt: {1: dict, 2: list, 3: dict and list}
        ''' 
        pass
        # return Boolean
    
    def keys(self, pattern='*', pt=3):
        ''' Returns an iterator over the keys matching the given pattern.
        
        Wildcards '?' and '*' can be used in the pattern.
        '''
        pass
        # return Iterator
    
    def flushdb(self, pt=3):
        ''' Removes everything from the namespace.
        
        pt: {1: dict, 2: list, 3: dict and list}
        '''
        pass
    
    
    ### Dict part
    
    def hset(self, k, f, v=None):
        ''' Set v to field f of k.
        
        If v is None, then f is expected as a dict, which will be expanded.
        '''
        pass
    
    def hget(self, k, f=None, v=None):
        ''' Get the value of field f from the key k.
        
        If f is not specified, a dict of all field-value pairs will be returned
        If v is given, return it when f or k does not exist.
        '''
        pass
        # return String or Dict
    
    def hexists(self, k, f):
        ''' Determine if field f under key k exists.
        
        '''
        # return Boolean
    
    def hdel(self, k, f):
        ''' Delete field f from key k.
        
        Returns True if field f exists, else False.
        '''
        # return Boolean
    
    
    ### List part
    
    def rpush(self, k, v):
        ''' Append value v to list k.
        
        The KvDB object ofter get initialized with a parameter `list_length'.
        When the list grow longer than the limit, it is stripped from the
        right side.
        Returns 0 when the list is not full before this push, which indicates 
        the push is a success. Returns -1 when the list is already full, which 
        indicates no value is pushed into the list.
        '''
        pass
        # return 0 or -1
    
    def rpop(self, k, v=None):
        ''' Remove and return the last element from list k
        
        Returns v if the list is empty.
        '''
        pass
        # return String or None
    
    def lpush(self, k, v):
        ''' Prepend value v to list k.
        
        The KvDB object ofter get initialized with a parameter `list_length'.
        When the list grow longer than the limit, it is stripped from the
        right side.
        Returns 0 when the list is not full before this push, which indicates 
        the push is a success. Returns -1 when the list is already full, which 
        indicates the new value has been pushed while the right most value has 
        been dropped.
        '''
        pass
        # return 0 or -1
    
    def lpop(self, k, v=None):
        ''' Remove and return the first element from list k
        
        Returns v if the list is empty.
        '''
        pass
        # return String or None
    
    def lrange(self, k, start=None, stop=None):
        ''' Return a slice of list k from the `start' position which is 
        
        included, to the `stop' position which is excluded. 
        Similar to the list slice notation of Python.
        '''
        pass
        # return Iterator
