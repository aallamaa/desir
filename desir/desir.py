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
import re
import pickle
import time

    
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


class Redis(object):
    """
    class providing a client interface to Redis
    this class is a minimalist implementation of
    http://code.google.com/p/redis/wiki/CommandReference
    except for the DEL command which is renamed delete
    because it is reserved in python
    """
    class redisCommand(object):
        def __init__(self,parent,name,arity,flag,vm_firstkey,vm_lastkey,vm_keystep):
            self.parent=parent
            self.name=name
            self.cmdname=name
            self.arity=int(arity)
            self.flag=flag
            self.vm_firstkey=vm_firstkey; # The first argument that's a key (0 = no keys) 
            self.vm_lastkey=vm_lastkey;  # THe last argument that's a key 
            self.vm_keystep=vm_keystep;  # The step between first and last key
            if self.name=="del":
                self.cmdname="delete"
            if self.name=="select":
                setattr(self,"runcmd",self._select)
            else:
                setattr(self,"runcmd",self._runcmd)

        def _runcmd(self,*args):
            return self.parent.runcmd(self.name,*args)

        def _select(self,*args):
            resp=self.parent.runcmd(self.name,*args)
            if resp=="OK":
                self.parent.db=int(args[0])
            return resp

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
        def __init__(self, name,timeout=0,fifo=True):
            self.name = name
            self.timeout = timeout
            self.fifo = fifo

        def __iter__(self):
            return self

        def send(self,name,val,timeout=0):
            return self._redis.rpush(name,pickle.dumps([self.name,time.time(),val]))

        def receive(self,timeout=0):
            if self.fifo:
                resp=self._redis.blpop(self.name,timeout)
            else:
                resp=self._redis.rlpop(self.name,timeout)
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
        self.redisCommands={}
        # Nodes to be used for cluster
        cmdfilter=re.compile('\{"(\w+)",(\w+),([-,\w]+),(\w+),(\w+),(\w+),([-,\w]+),(\w+)\}')
            
        for cmd in cmdfilter.findall(redisCommands):
            rc=self.redisCommand(self,cmd[0],cmd[2],cmd[3],cmd[4],cmd[5],cmd[6])
            setattr(self,rc.cmdname,rc.runcmd)
            self.redisCommands[rc.cmdname]=rc


    def runcmd(self,cmdname,*args):
        #cluster implementation to come soon after antirez publish the first cluster implementation
        return self.Nodes[0].runcmd(cmdname,*args)

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
            
redisCommands="""
    {"get",getCommand,2,0,NULL,1,1,1},
    {"set",setCommand,3,REDIS_CMD_DENYOOM,NULL,0,0,0},
    {"setnx",setnxCommand,3,REDIS_CMD_DENYOOM,NULL,0,0,0},
    {"setex",setexCommand,4,REDIS_CMD_DENYOOM,NULL,0,0,0},
    {"append",appendCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"substr",substrCommand,4,0,NULL,1,1,1},
    {"strlen",strlenCommand,2,0,NULL,1,1,1},
    {"del",delCommand,-2,0,NULL,0,0,0},
    {"exists",existsCommand,2,0,NULL,1,1,1},
    {"incr",incrCommand,2,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"decr",decrCommand,2,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"mget",mgetCommand,-2,0,NULL,1,-1,1},
    {"rpush",rpushCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"lpush",lpushCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"rpushx",rpushxCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"lpushx",lpushxCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"linsert",linsertCommand,5,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"rpop",rpopCommand,2,0,NULL,1,1,1},
    {"lpop",lpopCommand,2,0,NULL,1,1,1},
    {"brpop",brpopCommand,-3,0,NULL,1,1,1},
    {"blpop",blpopCommand,-3,0,NULL,1,1,1},
    {"llen",llenCommand,2,0,NULL,1,1,1},
    {"lindex",lindexCommand,3,0,NULL,1,1,1},
    {"lset",lsetCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"lrange",lrangeCommand,4,0,NULL,1,1,1},
    {"ltrim",ltrimCommand,4,0,NULL,1,1,1},
    {"lrem",lremCommand,4,0,NULL,1,1,1},
    {"rpoplpush",rpoplpushcommand,3,REDIS_CMD_DENYOOM,NULL,1,2,1},
    {"sadd",saddCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"srem",sremCommand,3,0,NULL,1,1,1},
    {"smove",smoveCommand,4,0,NULL,1,2,1},
    {"sismember",sismemberCommand,3,0,NULL,1,1,1},
    {"scard",scardCommand,2,0,NULL,1,1,1},
    {"spop",spopCommand,2,0,NULL,1,1,1},
    {"srandmember",srandmemberCommand,2,0,NULL,1,1,1},
    {"sinter",sinterCommand,-2,REDIS_CMD_DENYOOM,NULL,1,-1,1},
    {"sinterstore",sinterstoreCommand,-3,REDIS_CMD_DENYOOM,NULL,2,-1,1},
    {"sunion",sunionCommand,-2,REDIS_CMD_DENYOOM,NULL,1,-1,1},
    {"sunionstore",sunionstoreCommand,-3,REDIS_CMD_DENYOOM,NULL,2,-1,1},
    {"sdiff",sdiffCommand,-2,REDIS_CMD_DENYOOM,NULL,1,-1,1},
    {"sdiffstore",sdiffstoreCommand,-3,REDIS_CMD_DENYOOM,NULL,2,-1,1},
    {"smembers",sinterCommand,2,0,NULL,1,1,1},
    {"zadd",zaddCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"zincrby",zincrbyCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"zrem",zremCommand,3,0,NULL,1,1,1},
    {"zremrangebyscore",zremrangebyscoreCommand,4,0,NULL,1,1,1},
    {"zremrangebyrank",zremrangebyrankCommand,4,0,NULL,1,1,1},
    {"zunionstore",zunionstoreCommand,-4,REDIS_CMD_DENYOOM,zunionInterBlockClientOnSwappedKeys,0,0,0},
    {"zinterstore",zinterstoreCommand,-4,REDIS_CMD_DENYOOM,zunionInterBlockClientOnSwappedKeys,0,0,0},
    {"zrange",zrangeCommand,-4,0,NULL,1,1,1},
    {"zrangebyscore",zrangebyscoreCommand,-4,0,NULL,1,1,1},
    {"zrevrangebyscore",zrevrangebyscoreCommand,-4,0,NULL,1,1,1},
    {"zcount",zcountCommand,4,0,NULL,1,1,1},
    {"zrevrange",zrevrangeCommand,-4,0,NULL,1,1,1},
    {"zcard",zcardCommand,2,0,NULL,1,1,1},
    {"zscore",zscoreCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"zrank",zrankCommand,3,0,NULL,1,1,1},
    {"zrevrank",zrevrankCommand,3,0,NULL,1,1,1},
    {"hset",hsetCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"hsetnx",hsetnxCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"hget",hgetCommand,3,0,NULL,1,1,1},
    {"hmset",hmsetCommand,-4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"hmget",hmgetCommand,-3,0,NULL,1,1,1},
    {"hincrby",hincrbyCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"hdel",hdelCommand,3,0,NULL,1,1,1},
    {"hlen",hlenCommand,2,0,NULL,1,1,1},
    {"hkeys",hkeysCommand,2,0,NULL,1,1,1},
    {"hvals",hvalsCommand,2,0,NULL,1,1,1},
    {"hgetall",hgetallCommand,2,0,NULL,1,1,1},
    {"hexists",hexistsCommand,3,0,NULL,1,1,1},
    {"incrby",incrbyCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"decrby",decrbyCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"getset",getsetCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"mset",msetCommand,-3,REDIS_CMD_DENYOOM,NULL,1,-1,2},
    {"msetnx",msetnxCommand,-3,REDIS_CMD_DENYOOM,NULL,1,-1,2},
    {"randomkey",randomkeyCommand,1,0,NULL,0,0,0},
    {"select",selectCommand,2,0,NULL,0,0,0},
    {"move",moveCommand,3,0,NULL,1,1,1},
    {"rename",renameCommand,3,0,NULL,1,1,1},
    {"renamenx",renamenxCommand,3,0,NULL,1,1,1},
    {"expire",expireCommand,3,0,NULL,0,0,0},
    {"expireat",expireatCommand,3,0,NULL,0,0,0},
    {"keys",keysCommand,2,0,NULL,0,0,0},
    {"dbsize",dbsizeCommand,1,0,NULL,0,0,0},
    {"auth",authCommand,2,0,NULL,0,0,0},
    {"ping",pingCommand,1,0,NULL,0,0,0},
    {"echo",echoCommand,2,0,NULL,0,0,0},
    {"save",saveCommand,1,0,NULL,0,0,0},
    {"bgsave",bgsaveCommand,1,0,NULL,0,0,0},
    {"bgrewriteaof",bgrewriteaofCommand,1,0,NULL,0,0,0},
    {"shutdown",shutdownCommand,1,0,NULL,0,0,0},
    {"lastsave",lastsaveCommand,1,0,NULL,0,0,0},
    {"type",typeCommand,2,0,NULL,1,1,1},
    {"multi",multiCommand,1,0,NULL,0,0,0},
    {"exec",execCommand,1,REDIS_CMD_DENYOOM,execBlockClientOnSwappedKeys,0,0,0},
    {"discard",discardCommand,1,0,NULL,0,0,0},
    {"sync",syncCommand,1,0,NULL,0,0,0},
    {"flushdb",flushdbCommand,1,0,NULL,0,0,0},
    {"flushall",flushallCommand,1,0,NULL,0,0,0},
    {"sort",sortCommand,-2,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"info",infoCommand,1,0,NULL,0,0,0},
    {"monitor",monitorCommand,1,0,NULL,0,0,0},
    {"ttl",ttlCommand,2,0,NULL,1,1,1},
    {"persist",persistCommand,2,0,NULL,1,1,1},
    {"slaveof",slaveofCommand,3,0,NULL,0,0,0},
    {"debug",debugCommand,-2,0,NULL,0,0,0},
    {"config",configCommand,-2,0,NULL,0,0,0},
    {"subscribe",subscribeCommand,-2,0,NULL,0,0,0},
    {"unsubscribe",unsubscribeCommand,-1,0,NULL,0,0,0},
    {"psubscribe",psubscribeCommand,-2,0,NULL,0,0,0},
    {"punsubscribe",punsubscribeCommand,-1,0,NULL,0,0,0},
    {"publish",publishCommand,3,REDIS_CMD_FORCE_REPLICATION,NULL,0,0,0},
    {"watch",watchCommand,-2,0,NULL,0,0,0},
    {"unwatch",unwatchCommand,1,0,NULL,0,0,0}
"""
