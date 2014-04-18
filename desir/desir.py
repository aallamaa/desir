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
import os
import pickle
import time
import urllib2
import threading
import random
import json
from pkg_resources import resource_string
import __builtin__

redisCommands=None

def reloadCommands(url):
    global redisCommands
    try:
        u=urllib2.urlopen(url)
        redisCommands=json.load(u)
    except urllib2.HTTPError:
        raise(Exception("Error unable to load commmands json file"))


if "urlCommands" in dir(__builtin__):
    reloadCommands(__builtin__.urlCommands)

#urlCommands="https://raw.github.com/antirez/redis-doc/master/commands.json"
#reloadCommands(urlCommands) uncomment if you want to update it at each import

if not redisCommands:
    try:
        redisCommands=json.loads(resource_string(__name__,"commands.json"))
    except IOError:
        raise(Exception("Error unable to load commmands json file"))

class SWM(dict):
	"""
	Connector message
	"""
	def __init__(self, initd=None):
		if initd is None:
			initd = {}
		dict.__init__(self, initd)

	def __getattr__(self, item):
            try:
                d = self.__getitem__(item)
            except KeyError:
                raise(AttributeError)
            # if value is the only key in object, you can omit it
            if isinstance(d, dict) and 'value' in d and len(d) == 1:
                return d['value']
            else:
                return d

	def __setattr__(self, item, value):
		self.__setitem__(item, value)
    
class RedisError(Exception):
    pass

class NodeError(Exception):
    pass

class ConnectorError(Exception):
    pass

class RedisInner(object):
  def __init__(self, cls):
    self.cls = cls
  def __get__(self, instance, outerclass):
    class Wrapper(self.cls):
      _redis = instance
    Wrapper.__name__ = self.cls.__name__
    return Wrapper

cmdmap={"del":"delete","exec":"execute"}

class MetaRedis(type):
       
       def __new__(metacls, name, bases, dct):
        def _wrapper(name,redisCommand,methoddct):
            
            runcmd="runcmd"
            if name=="SELECT":
                runcmd="_select"

            def _rediscmd(self, *args):
                return methoddct[runcmd](self, name, *args)

            _rediscmd.__name__= cmdmap.get(name.lower(),str(name.lower().replace(" ","_")))
            _rediscmd.__redisname__= name
            _rediscmd._json = redisCommand
            if redisCommand.has_key("summary"):
                _doc = redisCommand["summary"]
                if redisCommand.has_key("arguments"):
                    _doc+="\nParameters:\n"
                    for d in redisCommand["arguments"]:
                        _doc+="Name: %s,\tType: %s,\tMultiple parameter:%s\n" % (d["name"],d["type"],d.get("multiple","False"))             
                _rediscmd.__doc__  = _doc
            _rediscmd.__dict__.update(methoddct[runcmd].__dict__)
            return _rediscmd

        if name != "Redis":
            return type.__new__(metacls, name, bases, dct)

        newDct = {}
        for k in redisCommands.keys():
            newDct[cmdmap.get(k.lower(),str(k.lower().replace(" ","_")))]= _wrapper(k,redisCommands[k],dct)
        newDct.update(dct)
        return type.__new__(metacls, name, bases, newDct)


class SubAsync(threading.Thread):
    def __init__(self,channel,callback):
        threading.Thread.__init__(self)
        self.setDaemon(1)
        self.channel=channel
        self.callback=callback
        self.start()
    def run(self):
        import desir
        self._redis=desir.Redis()
        self._redis.subscribe(self.channel)
        for v in self._redis.listen():
            self.callback(v)



class Redis(threading.local):
    """
    class providing a client interface to Redis
    this class is a minimalist implementation of
    http://code.google.com/p/redis/wiki/CommandReference
    except for the DEL and EXEC command which are renamed delete and execute
    because they are reserved names in python
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
            if seed!=None:
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
        safe queue implementation under work
        CONNECTORNAME:PID:TIMESTAMP
        """
        def __init__(self, name,ctype="",timeout=0,fifo=True,safe=False,secret=None):
            self.name = name
            self.timeout = timeout
            # connector queue/list set to fifo when fifo is true, and lifo when false
            self.fifo = fifo
            # when safe is true, connector works in safe mode meaning each time
            # a value is popped out of the list it is atomically pushed to a dedicated list
            # when the program is done processing the object that was popped out,
            # it can release it with the release commands which will remove it from the
            # dedicated list
            self.safe = safe
            self.ctype = ctype
            self.secret = secret

        def __iter__(self):
            return self

        def sendreceive(self,name,val=None,timeout=0):
            srcreply=self.name+":"+str(time.time())+str(random.random())
            self.send(name,val,srcreply)
            return self.receive(timeout=timeout,srcreply=srcreply)

        def send(self,name,val=None,srcreply=None):
            if srcreply:
                vd=SWM(dict(src=srcreply,srctype=self.ctype,dst=name,time=time.time(),val=val))
            else:
                vd=SWM(dict(src=self.name,srctype=self.ctype,dst=name,time=time.time(),val=val))
            #if not val:
            #    vd.update(name)
            vp=pickle.dumps(vd)
            if self.secret:
                import hashlib
                vs=hashlib.sha1()
                vs.update(vp)
                vs.update(self.secret)
                vp=vs.digest()+vp
            if self.fifo:
                return self._redis.lpush(vd.dst,vp)
            else:
                return self._redis.rpush(vd.dst,vp)

        def receive(self,timeout=0,srcreply=None):
            tmpname="%s:%d:%d" % (self.name,os.getpid(),int(time.time()))
            if srcreply==None:
                srcreply=self.name
            if self.safe:
                if timeout==-1:
                    resp=self._redis.rpoplpush(srcreply,tmpname)
                else:
                    resp=self._redis.brpoplpush(srcreply,tmpname,timeout)
            else:
                if timeout==-1:
                    resp=self._redis.rpop(srcreply)
                else:
                    resp=self._redis.brpop(srcreply,timeout)
                    resp=resp and resp[1]
            if resp:
                if self.secret:
                    import hashlib
                    vs=hashlib.sha1()
                    vs.update(resp[20:])
                    vs.update(self.secret)
                    if resp[:20] != vs.digest():
                        raise ConnectorError("Digest signature failed")
                    resp=resp[20:]
                resp=pickle.loads(resp)
                if self.safe:
                    resp["srcack"]=tmpname
            return resp

        def unreceive(self,val):
            if val.has_key("srcack"):
                return self._redis.rpoplpush(val.srcack,self.name)

        def transfer(self,name,val,newval,force=True):
            res=None
            while not res:
                self._redis.watch(val.srcack)
                self._redis.multi()
                self.release(val)
                self.send(name,newval)
                res=self._redis.execute()
                if not force:
                    break
            return res

        def reply(self,val,newval,force=True):
            res=None
            while not res:
                if val.has_key("srcack"):
                    self._redis.watch(val.srcack)
                self._redis.multi()
                self.release(val)
                self.send(val.src,newval)
                res=self._redis.execute()
                if not force:
                    break
            return res

                    

        def release(self,val):
            if val.has_key("srcack"):
                return self._redis.rpop(val.srcack)
            

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
    
    def __init__(self,host="localhost",port=6379,db=0,password=None,timeout=None,safe=False):
        self.host=host
        self.port=port
        self.timeout=timeout
        self.db=db
        self.password=password
        self.safe=safe
        self.safewait=0.1
        self.Nodes=[Node(host,port,db,password,timeout)]
        self.transaction=False
        self.subscribed=False

    def listen(self,todict=False):
        while self.subscribed:
            r = self.Nodes[0].parse_resp()
            if r[0] == 'unsubscribe' and r[2] == 0:
                self.subscribed = False
            if todict:
                if r[0]=="pmessage":
                    r=dict(type=r[0],pattern=r[1],channel=r[2],data=r[3])
                else:
                    r=dict(type=r[0],pattern=None,channel=r[1],data=r[2])
            yield r


    def runcmd(self,cmdname,*args):
        #cluster implementation to come soon after antirez publish the first cluster implementation
        if cmdname in ["MULTI","WATCH"]:
            self.transaction=True
        if self.safe and not self.transaction and not self.subscribed:
            try:
                return self.Nodes[0].runcmd(cmdname,*args)
            except NodeError:
                time.sleep(self.safewait)

        if cmdname in ["DISCARD","EXEC"]:
            self.transaction=False
        try:
            if cmdname in ["SUBSCRIBE","PSUBSCRIBE","UNSUBSCRIBE","PUNSUBSCRIBE"]:
                self.Nodes[0].sendcmd(cmdname,*args)
                rsp = self.Nodes[0].parse_resp()
            else:
                rsp = self.Nodes[0].runcmd(cmdname,*args)
            if cmdname in ["SUBSCRIBE","PSUBSCRIBE"]:
                self.subscribed = True
            return rsp
        except NodeError as e:
            self.transaction=False
            self.subscribed=False
            raise NodeError(e)


    def _select(self,cmdname,*args):
        resp=self.runcmd(cmdname,*args)
        if resp=="OK":
            self.db=int(args[0])
        return resp


    def runcmdon(self,node,cmdname,*args):
        return self.node.runcmd(cmdname,*args)


    
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

        addrinfo = socket.getaddrinfo(self.host, self.port)
        addrinfo.sort(key=lambda x: 0 if x[0] == socket.AF_INET else 1)
        family, _, _, _, _ = addrinfo[0]

        sock = socket.socket(family, socket.SOCK_STREAM)
        try:
            sock.connect((self.host, self.port))
            sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(self.timeout)
            self._sock = sock
            self._fp = sock.makefile('r')

        except socket.error as msg:
            if len(msg.args)==1:
                raise NodeError("Error connecting %s:%s. %s." % (self.host,self.port,msg.args[0]))
            else:
                raise NodeError("Error %s connecting %s:%s. %s." % (msg.args[0],self.host,self.port,msg.args[1]))

        finally:
            if self.password:
                if not self.runcmd("auth",self.password):
                    raise RedisError("Authentication error: Invalid password")
            if self._sock:
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
        except socket.error as msg:
            self.disconnet()
            if len(msg.args)==1:
                raise NodeError("Error connecting %s:%s. %s." % (self.host,self.port,msg.args[0]))
            else:
                raise NodeError("Error %s connecting %s:%s. %s." % (msg.args[0],self.host,self.port,msg.args[1]))
       

    def readline(self):
        try:
            return self._fp.readline()
        except socket.error as msg:
            self.disconnect()
            if len(msg.args)==1:
                raise NodeError("Error connecting %s:%s. %s." % (self.host,self.port,msg.args[0]))
            else:
                raise NodeError("Error %s connecting %s:%s. %s." % (msg.args[0],self.host,self.port,msg.args[1]))


    def sendline(self,message):
        self.connect()
        try:
            self._sock.send(message+"\r\n")
        except socket.error as msg:
            self.disconnect()
            if len(msg.args)==1:
                raise NodeError("Error connecting %s:%s. %s." % (self.host,self.port,msg.args[0]))
            else:
                raise NodeError("Error %s connecting %s:%s. %s." % (msg.args[0],self.host,self.port,msg.args[1]))


    def sendcmd(self,*args):
        args2=args[0].split()
        args2.extend(args[1:])
        cmd=""
        cmd+="*%d" % (len(args2))
        for arg in args2:
            cmd+="\r\n"
            cmd+="$%d\r\n" % (len(str(arg)))
            cmd+=str(arg)
        self.sendline(cmd)
    

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



