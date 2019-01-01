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


import socket
import time
import urllib
import threading
import json
from pkg_resources import resource_string
import builtins
from .sugar import Counter, String, Connector, Hash

redisCommands = None


def reloadCommands(url):
    global redisCommands
    try:
        u = urllib.request.urlopen(url)
        redisCommands = json.load(u)
    except urllib.request.HTTPError:
        raise Exception("Error unable to load commmands json file")


if "urlCommands" in dir(builtins):
    reloadCommands(builtins.urlCommands)

# uncomment the following section if you want to force a reload at each import
# urlCommands = \
# "https://raw.githubusercontent.com/antirez/redis-doc/master/commands.json"
# reloadCommands(urlCommands)

if not redisCommands:
    try:
        redisCommands = json.loads(
            resource_string(__name__, "commands.json").decode("utf-8"))
    except IOError:
        raise Exception("Error unable to load commmands json file")


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


# commands name which requires renaming
cmdmap = {"del": "delete", "exec": "execute"}


class MetaRedis(type):
    def __new__(metacls, name, bases, dct):
        def _wrapper(name, redisCommand, methoddct):
            runcmd = "runcmd"
            if name == "SELECT":
                runcmd = "_select"

            def _rediscmd(self, *args):
                return methoddct[runcmd](self, name, *args)

            _rediscmd.__name__ = cmdmap.get(
                name.lower(), str(name.lower().replace(" ", "_")))
            _rediscmd.__redisname__ = name
            _rediscmd._json = redisCommand
            if "summary" in redisCommand:
                _doc = redisCommand["summary"]
                if "arguments" in redisCommand:
                    _doc += "\nParameters:\n"
                    for d in redisCommand["arguments"]:
                        if "name" in d:
                            _doc += ("Name: %s,\tType: %s,\t"
                                     "Multiple parameter:%s\n") % (
                                         d["name"], d.get("type", "?"),
                                         d.get("multiple", "False"))
                _rediscmd.__doc__ = _doc
            _rediscmd.__dict__.update(methoddct[runcmd].__dict__)
            return _rediscmd

        if name != "Redis":
            return type.__new__(metacls, name, bases, dct)

        newDct = {}
        for k in redisCommands.keys():
            newDct[cmdmap.get(k.lower(), str(k.lower().replace(" ", "_")))] = \
                _wrapper(k, redisCommands[k], dct)
        newDct.update(dct)
        return type.__new__(metacls, name, bases, newDct)


class Redis(threading.local, metaclass=MetaRedis):
    """
    class providing a client interface to Redis
    this class is a minimalist implementation of
    http://code.google.com/p/redis/wiki/CommandReference
    except for the DEL and EXEC command which are renamed delete and execute
    because they are reserved names in python
    """

    String = RedisInner(String)
    Counter = RedisInner(Counter)
    Connector = RedisInner(Connector)
    Hash = RedisInner(Hash)

    def __init__(self, host="localhost", port=6379, db=0,
                 password=None, timeout=None, safe=False):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.db = db
        self.password = password
        self.safe = safe
        self.safewait = 0.1
        self.Nodes = [Node(host, port, db, password, timeout)]
        self.transaction = False
        self.subscribed = False

    def listen(self, todict=False):
        while self.subscribed:
            r = self.Nodes[0].parse_resp()
            if r[0] == 'unsubscribe' and r[2] == 0:
                self.subscribed = False
            if todict:
                if r[0] == "pmessage":
                    r = dict(type=r[0], pattern=r[1], channel=r[2], data=r[3])
                else:
                    r = dict(type=r[0], pattern=None, channel=r[1], data=r[2])
            yield r

    def runcmd(self, cmdname, *args):
        # cluster implementation to come soon after antirez publish
        # the first cluster implementation
        # this is a comment from 2010 so i guess some work as to be done here..
        if cmdname in ["MULTI", "WATCH"]:
            self.transaction = True
        if self.safe and not self.transaction and not self.subscribed:
            try:
                return self.Nodes[0].runcmd(cmdname, *args)
            except NodeError:
                time.sleep(self.safewait)

        if cmdname in ["DISCARD", "EXEC", "UNWATCH"]:
            self.transaction = False
        try:
            if cmdname in ["SUBSCRIBE", "PSUBSCRIBE",
                           "UNSUBSCRIBE", "PUNSUBSCRIBE"]:
                self.Nodes[0].sendcmd(cmdname, *args)
                rsp = self.Nodes[0].parse_resp()
            else:
                rsp = self.Nodes[0].runcmd(cmdname, *args)
            if cmdname in ["SUBSCRIBE", "PSUBSCRIBE"]:
                self.subscribed = True
            return rsp
        except NodeError as e:
            self.transaction = False
            self.subscribed = False
            raise NodeError(e)

    def pipeline(self):
        self.multi()
        return self

    def _select(self, cmdname, *args):
        resp = self.runcmd(cmdname, *args)
        if resp == "OK":
            self.db = int(args[0])
        return resp

    def runcmdon(self, node, cmdname, *args):
        return self.node.runcmd(cmdname, *args)


class Node(object):
    """
    Manage TCP connections to a redis node
    """

    def __init__(self, host="localhost", port=6379, db=0,
                 password=None, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.password = password
        self._sock = None
        self._fp = None
        self.db = db

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
            self._fp = sock.makefile('rb')

        except socket.error as msg:
            if len(msg.args) == 1:
                raise NodeError("Error connecting %s:%s. %s." % (
                    self.host, self.port, msg.args[0]))
            else:
                raise NodeError("Error %s connecting %s:%s. %s." % (
                    msg.args[0], self.host, self.port, msg.args[1]))

        finally:
            if self.password:
                if not self.runcmd("auth", self.password):
                    raise RedisError("Authentication error: Invalid password")
            if self._sock:
                self.runcmd("select", "0")

    def disconnect(self):
        if self._sock:
            try:
                self._sock.close()
            except socket.error:
                pass
            finally:
                self._sock = None
                self._fp = None

    def read(self, length):
        try:
            return self._fp.read(length)
        except socket.error as msg:
            self.disconnet()
            if len(msg.args) == 1:
                raise NodeError("Error connecting %s:%s. %s." % (
                    self.host, self.port, msg.args[0]))
            else:
                raise NodeError("Error %s connecting %s:%s. %s." % (
                    msg.args[0], self.host, self.port, msg.args[1]))

    def readline(self):
        try:
            return self._fp.readline()
        except socket.error as msg:
            self.disconnect()
            if len(msg.args) == 1:
                raise NodeError("Error connecting %s:%s. %s." % (
                    self.host, self.port, msg.args[0]))
            else:
                raise NodeError("Error %s connecting %s:%s. %s." % (
                    msg.args[0], self.host, self.port, msg.args[1]))

    def sendline(self, message):
        self.connect()
        try:
            self._sock.send(message+b"\r\n")
        except socket.error as msg:
            self.disconnect()
            if len(msg.args) == 1:
                raise NodeError("Error connecting %s:%s. %s." % (
                    self.host, self.port, msg.args[0]))
            else:
                raise NodeError("Error %s connecting %s:%s. %s." % (
                    msg.args[0], self.host, self.port, msg.args[1]))

    def sendcmd(self, *args):
        args2 = args[0].split()
        args2.extend(args[1:])
        cmd = b""
        cmd += b"*%d" % (len(args2))
        for carg in args2:
            arg = None
            if type(carg) is bytes:
                arg = carg
            else:
                arg = bytes(str(carg), "utf-8")
            cmd += b"\r\n"
            cmd += b"$%d\r\n" % (len(arg))
            cmd += arg
        self.sendline(cmd)

    def parse_resp(self):
        resp = self.readline()
        if not resp:
            # resp empty what is happening ? to be investigated
            return None
        if resp[:-2] in [b"$-1", b"*-1"]:
            return None
        fb, resp = resp[0], resp[1:]
        if fb == 43:  # +
            return resp[:-2]
        if fb == 45:  # -
            raise RedisError(resp.decode("UTF-8").strip())
        if fb == 58:  # :
            return int(resp)
        if fb == 36:  # $
            if int(resp) != -1:
                resp = self.read(int(resp))
                self.read(2)
                return resp
            else:
                return None
        if fb == 42:  # *
            return [self.parse_resp() for i in range(int(resp))]

    def runcmd(self, cmdname, *args):
        self.sendcmd(cmdname, *args)
        return self.parse_resp()


class SubAsync(threading.Thread):
    def __init__(self, channel, callback, **redis_param):
        threading.Thread.__init__(self)
        self.setDaemon(1)
        self.channel = channel
        self.callback = callback
        self.param = redis_param
        self.start()

    def run(self):
        self._redis = Redis(**self.param)
        self._redis.subscribe(self.channel)
        for v in self._redis.listen():
            self.callback(v)
