# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import

import struct

class Protocol(object):
    def __init__(self, socket, code):
        self.socket = socket
        self.code = code
        self.wasread = False
        self.copy_context = False
        
        self.size_frame = struct.Struct('> I')
        self.header_frame = struct.Struct('> B')
        
    def read(self):
        pass
    
    def next_header(self):
        value = self.socket.recv(1)
        
        return self.header_frame.unpack(value)[0]
    
    def next_size(self):
        value = self.socket.recv(4)
        
        return self.size_frame.unpack(value)[0]
        
    def next_message(self, size):
        val = self.socket.recv(size)
        return str(val.decode('utf8'))


class RowSet(Protocol):
    inserted = 0
    deleted = 1
    end = 2
    item = 3
    
    def __init__(self, socket):
        Protocol.__init__(self, socket, 0)
        self.message = []
    
    def read(self):
        if self.wasread:
            return
        self.wasread = True
        
        token = self.next_header()
        
        while token != RowSet.end:
            nval = []
            
            if token == RowSet.inserted:
            	nval.append(u'inserted')
            elif token == RowSet.deleted:
            	nval.append(u'deleted')
            
            if len(nval) == 1:
            	type = self.next_header()
            	
            	while type == RowSet.item:
            		
            		sz = self.next_size()
            		
            		msg = self.next_message(sz)
            		
            		nval.append(msg)
            		
            		type = self.next_header()
            	
            	self.message.append(tuple(nval))
            	token = type
            else:
            	print(u'No row found.')
            	token = self.next_header()

class NewDatabase(Protocol):
    def __init__(self, socket):
        Protocol.__init__(self, socket, 1)
        self.message = ''
        self.database = ''
        
    
    def read(self):
        if self.wasread:
            return
        self.wasread = True
        
        size = self.next_size()
        self.database = self.next_message(size)
        size = self.next_size()
        self.message = self.next_message(size)


class CopySuccess(Protocol):
    def __init__(self, socket):
        Protocol.__init__(self, socket, 2)
        self.message = u''
        self.copy_context = True
    
    def read(self):
        if self.wasread:
            return
        self.wasread = True
        
        size = self.next_size()
        self.message = self.next_message(size)

class Success(Protocol):
    def __init__(self, socket):
        Protocol.__init__(self, socket, 3)
        self.message = u''
    
    def read(self):
        if self.wasread:
            return
        self.wasread = True
        
        size = self.next_size()
        self.message = self.next_message(size)

class Error(Protocol):
    def __init__(self, socket):
        Protocol.__init__(self, socket, 4)
        self.message = u''
    
    def read(self):
        if self.wasread:
            return
        self.wasread = True
        
        size = self.next_size()
        self.message = self.next_message(size)
    
class InternalError(Protocol):
    def __init__(self, socket):
        Protocol.__init__(self, socket, 4)
        
        self.message = u"Unrecognized type: " + self.socket.recv(4096)

protocol = {
	0: RowSet,
    1: NewDatabase,
    2: CopySuccess,
    3: Success,
    4: Error,
}
