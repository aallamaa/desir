import socket
import re

    
class RedisError(Exception):
    pass

class NodeError(Exception):
    pass

class Node(object):
    """
    Manage TCP connections to a redis node
    """
    def __init__(self,host="localhost",port=6379,db=0,timeout=None):
        self.host=host
        self.port=port
        self.timeout=timeout
        self._sock=None
        self._fp=None
        self.db=db
        cmdfilter=re.compile('\{"(\w+)",(\w+),([-,\w]+),(\w+),(\w+),(\w+),([-,\w]+),(\w+)\}')
        class redisCommand(object):
            def __init__(self,self2,name,arity,flag,vm_firstkey,vm_lastkey,vm_keystep):
                self.self2=self2
                self.name=name
                self.arity=int(arity)
                self.flag=flag
                self.vm_firstkey=vm_firstkey; # The first argument that's a key (0 = no keys) 
                self.vm_lastkey=vm_lastkey;  # THe last argument that's a key 
                self.vm_keystep=vm_keystep;  # The step between first and last key 

            def runcmd(self,*args):
                return self.self2.runcmd(self.name,*args)
            
        for cmd in cmdfilter.findall(redisCommands):
            rc=redisCommand(self,cmd[0],cmd[2],cmd[3],cmd[4],cmd[5],cmd[6])
            setattr(self,cmd[0],rc.runcmd)

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
        if cmdname=="select":
            resp=self.parse_resp()
            if resp=="OK":
                self.db=int(args[0])
                return resp
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
