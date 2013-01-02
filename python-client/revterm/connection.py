# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import

import socket
import struct

try:
    from .protocol import *
except:
    from protocol import *

class RevoriConnection(object):
    def __init__(self, host='localhost', port=8017):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        self.socket.connect((host, port))
        self.frame = struct.Struct('> B I')
        self.header = struct.Struct('> B')
    
    def send(self, msg):
        smsg = self.frame.pack(*(0, len(msg))) + msg.encode('utf8')
        
        self.socket.sendall(smsg)
        
        
    def recv(self):
        value = self.socket.recv(1)
        t = self.header.unpack(value)[0]
        
        p = protocol.get(t, InternalError)
            
        retval = p(self.socket)
        retval.read()
        
        return retval
        
    def close(self):
        self.socket.close()

