#
# Copyright (c) 2010, Abdelkader ALLAM <abdelkader.allam at gmail dot com>
# All rights reserved.
#
# This source also contains source code from Redis
# developped by Salvatore Sanfilippo <antirez at gmail dot com>
# available at http://github.com/antirez/redis
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#    * Redistributions of source code must retain the above copyright notice,
#      this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of Redis nor the names of its contributors may be used
#      to endorse or promote products derived from this software without
#      specific prior written permission.
# 
#  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
#  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
#  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
#  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
#  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
#  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
#  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
#  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
#  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
#  POSSIBILITY OF SUCH DAMAGE.
# 

import socket
import types
import pickle
import time
import urllib2
import json
from pkg_resources import resource_string


redisCommands=None
url="https://github.com/antirez/redis-doc/raw/master/commands.json"
try:
    u=urllib2.urlopen(url)
    redisCommands=json.load(u)
except:
    pass

if not redisCommands:
    try:
        redisCommands=json.loads(resource_string(__name__,"commands.json"))
    except IOError:
        raise(Exception("Error unable to load commmands json file"))


    
class RedisError(Exception):
    pass

class NodeError(Exception):
    pass


class RedisInner(object):
  def __init__(self, cls):
    self.cls = cls
  def __get__(self, instance, outerclass):
    class Wrapper(self.cls):
      _redis = instance
    Wrapper.__name__ = self.cls.__name__
    return Wrapper


class MetaRedis(type):
       def __new__(metacls, name, bases, dct):
        def _wrapper(name,redisCommand,methoddct):
            
            runcmd="runcmd"
            if name=="SELECT":
                runcmd="_select"

            def _trace(self, *args):
                return methoddct[runcmd](self, name, *args)

            _trace.__name__ = str(name.lower())
            if name=="del":
                _trace.__name__ = "delete"
            if name=='exec':
                _trace.__name__ = "execute"

            if redisCommand.has_key("summary"):
                _trace.__doc__  = redisCommand["summary"]
            _trace.__dict__.update(methoddct[runcmd].__dict__)
            return _trace

        newDct = {}
        for k in redisCommands.keys():
            newDct[k.lower()]= _wrapper(k,redisCommands[k],dct)
        newDct.update(dct)
        return type.__new__(metacls, name, bases, newDct)

class Redis(object):
    """
    class providing a client interface to Redis
    this class is a minimalist implementation of
    http://code.google.com/p/redis/wiki/CommandReference
    except for the DEL and EXEC command which is renamed delete and execute
    because it is reserved in python
    """
    __metaclass__ = MetaRedis
 
    class String(object):
        """
        Redis String descriptor object
        """
        def __init__(self,name):
            self.name=name

        def __get__(self,instance,owner):

            return [self,instance,owner,self._redis.get(self.name)]
        
        def __set__(self, instance, value):
            return self._redis.set(self.name,value)
    String=RedisInner(String)


    class Counter:
        def __init__(self, name, seed=0):
            self.name = name
            self._redis.set(self.name,seed)

        def __iter__(self):
            return self

        def __int__(self):
            return int(self._redis.get(self.name))

        def __str__(self):
            return self._redis.get(self.name)

        def next(self):
            return self._redis.incr(self.name)
    Counter=RedisInner(Counter)

    class Connector(object):
        """
        
        CONNECTORNAME:UNIQUEID:TIMESTAMP
        """
        def __init__(self, name,timeout=0,fifo=True,safe=False):
            self.name = name
            self.timeout = timeout
            self.fifo = fifo
            self.safe = safe

        def __iter__(self):
            return self

        def send(self,name,val,timeout=0):
            if self.fifo:
                return self._redis.lpush(name,pickle.dumps([self.name,time.time(),val]))
            else:
                return self._redis.rpush(name,pickle.dumps([self.name,time.time(),val]))

        def receive(self,timeout=0):
            if self.safe:
                tmpname="%s:%d:%d" % (self.name,int(time.time()),0)
                resp=self._redis.brpoplpush(self.name,tmpname,timeout)
            else:
                resp=self._redis.brpop(self.name,timeout)
            if resp:
                return pickle.loads(resp[1])

        def next(self):
            resp = self.receive(self.timeout)
            if resp:
                return resp
            else:
                raise StopIteration
    Connector=RedisInner(Connector)
    
    class Hash(object):
        def __init__(self, name):
            self._keyid=name

        def __repr__(self):
            return str(self.items())

	def __getattr__(self, item):
            if item.startswith("_"):
                return object.__getattribute__(self, item)
            resp=self._redis.hget(self._keyid,item)
            if resp:
                return resp
            else:
                raise AttributeError("Unkown attribute %s for object %s" % (item,self._keyid))

	def __setattr__(self, item, value):
            if item.startswith("_"):
                return  object.__setattr__(self, item, value)
            else:
                self._redis.hset(self._keyid,item,value)

        def keys(self):
            return self._redis.hkeys(self._keyid)

        def values(self):
            return self._redis.hvals(self._keyid)

        def items(self):
            resp=self._redis.hgetall(self._keyid)
            if resp:
                return zip(resp[::2],resp[1::2])

    Hash=RedisInner(Hash)
    
    def __init__(self,host="localhost",port=6379,db=0,password=None,timeout=None):
        self.host=host
        self.port=port
        self.timeout=timeout
        self.db=db
        self.password=password
        self.Nodes=[Node(host,port,db,password,timeout)]



    def runcmd(self,cmdname,*args):
        #cluster implementation to come soon after antirez publish the first cluster implementation
        return self.Nodes[0].runcmd(cmdname,*args)

    def _select(self,cmdname,*args):
        resp=self.runcmd(cmdname,*args)
        if resp=="OK":
            self.db=int(args[0])
        return resp


    def runcmdon(self,node,cmdname,*args):
        return self.node.runcmd(cmdname,*args)

    def renamecommand(self,name,newname,newfuncname=None):
        if not self.redisCommands.has_key(name):
            return False
        rc=self.redisCommands[name]
        rc.name=newname
        if newfuncname:
            setattr(self,rc.name,rc.runcmd)

    
class Node(object):
    """
    Manage TCP connections to a redis node
    """

    def __init__(self,host="localhost",port=6379,db=0,password=None,timeout=None):
        self.host=host
        self.port=port
        self.timeout=timeout
        self.password=password
        self._sock=None
        self._fp=None
        self.db=db

    def connect(self):
        if self._sock:
            return
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
        except socket.error, msg:
            if len(msg.args)==1:
                raise NodeError("Error connecting %s:%s. %s." % (self.host,self.port,msg.args[0]))
            else:
                raise NodeError("Error %s connecting %s:%s. %s." % (msg.args[0],self.host,self.port,msg.args[1]))

        finally:
            sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(self.timeout)
            self._sock = sock
            self._fp = sock.makefile('r')
            if self.password:
                if not self.runcmd("auth",self.password):
                    raise RedisError("Authentication error: Invalid password")
            self.runcmd("select","0")

    def disconnect(self):
        if self._sock:
            try:
                self._sock.close()
            except socket.error:
                pass
            finally:
                self._sock=None
                self._fp=None
        
    def read(self,length):
        try:
            return self._fp.read(length)
        except socket.error, msg:
            if len(msg.args)==1:
                raise NodeError("Error connecting %s:%s. %s." % (self.host,self.port,msg.args[0]))
            else:
                raise NodeError("Error %s connecting %s:%s. %s." % (msg.args[0],self.host,self.port,msg.args[1]))
       

    def readline(self):
        try:
            return self._fp.readline()
        except socket.error, msg:
            if len(msg.args)==1:
                raise NodeError("Error connecting %s:%s. %s." % (self.host,self.port,msg.args[0]))
            else:
                raise NodeError("Error %s connecting %s:%s. %s." % (msg.args[0],self.host,self.port,msg.args[1]))


    def sendline(self,message):
        self.connect()
        try:
            self._sock.send(message+"\r\n")
        except socket.error, msg:
            if len(msg.args)==1:
                raise NodeError("Error connecting %s:%s. %s." % (self.host,self.port,msg.args[0]))
            else:
                raise NodeError("Error %s connecting %s:%s. %s." % (msg.args[0],self.host,self.port,msg.args[1]))


    def sendcmd(self,*args):
        self.sendline("*%d " % (len(args)))
        for arg in args:
            self.sendline("$%d" % (len(str(arg))))
            self.sendline(str(arg))
    

    def parse_resp(self):
        resp = self.readline()
        if not resp:
            # resp empty what is happening ? to be investigated
            return None
        if resp[:-2] in ["$-1","*-1"]:
            return None
        fb,resp=resp[0],resp[1:]
        if fb=="+":
            return resp[:-2]
        if fb=="-":
            raise RedisError(resp)
        if fb==":":
            return int(resp)
        if fb=="$":
            resp=self.read(int(resp))
            self.read(2)
            return resp
        if fb=="*":
            return [self.parse_resp() for i in range(int(resp))]

    def runcmd(self,cmdname,*args):
        self.sendcmd(cmdname,*args)
        return self.parse_resp()



