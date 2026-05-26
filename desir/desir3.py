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
import urllib.request
import urllib.error
import threading
import json
from importlib.resources import files
import builtins
from .sugar import Counter, String, Connector, Hash

redisCommands = None

DEFAULT_SENTINEL_TIMEOUT = 0.1

# Canonical location of the upstream command description file. The historical
# antirez/redis-doc repository now redirects here.
COMMANDS_URL = \
    "https://raw.githubusercontent.com/redis/redis-doc/master/commands.json"


def parse_version(s):
    """Turn a version string like ``"8.0.2"`` into a comparable ``(8, 0, 2)``
    tuple. Non-numeric trailing parts are ignored, missing parts become 0."""
    parts = []
    for chunk in str(s).split("."):
        digits = ""
        for ch in chunk:
            if ch.isdigit():
                digits += ch
            else:
                break
        parts.append(int(digits) if digits else 0)
    return tuple(parts) if parts else (0,)


def reloadCommands(url):
    global redisCommands
    try:
        u = urllib.request.urlopen(url)
        redisCommands = json.load(u)
    except (urllib.error.URLError, ValueError) as e:
        raise Exception(
            "Error unable to load commmands json file: %s" % e)


if "urlCommands" in dir(builtins):
    reloadCommands(builtins.urlCommands)

# uncomment the following section if you want to force a reload at each import
# reloadCommands(COMMANDS_URL)

if not redisCommands:
    try:
        redisCommands = json.loads(
            files(__package__).joinpath("commands.json").read_text("utf-8"))
    except OSError:
        raise Exception("Error unable to load commmands json file")


class RedisError(Exception):
    pass


class NodeError(Exception):
    pass

class SentinelErrorNoMaster(Exception):
    pass

class SentinelError(Exception):
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


def command_method_name(redis_name):
    """Map a Redis command name (e.g. ``"GET"``, ``"DEL"``, ``"CONFIG GET"``)
    to the Python method name exposed on the client."""
    lowered = redis_name.lower()
    return cmdmap.get(lowered, str(lowered.replace(" ", "_")))

class MetaRedis(type):
    def __new__(metacls, name, bases, dct):
        def _wrapper(name, redisCommand, methoddct):
            runcmd = "runcmd"
            if name == "SELECT":
                runcmd = "_select"

            def _rediscmd(self, *args):
                return methoddct[runcmd](self, name, *args)

            _rediscmd.__name__ = command_method_name(name)
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
            newDct[command_method_name(k)] = _wrapper(k, redisCommands[k], dct)
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
                 password=None, timeout=None, safe=False, sentinels=None, service_name=None,
                 debug=False):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.db = db
        self.password = password
        self.safe = safe
        self.safewait = 0.1
        self.debug = debug
        if sentinels:
            self.sentinels = [Node(host, port, 0, None, timeout or DEFAULT_SENTINEL_TIMEOUT)
                              for host,port in sentinels]
            if service_name:
                self.service_name = service_name
            else:
                self.service_name = None
                for node in self.sentinels:
                    res = node.runcmd('sentinel', 'masters')
                    if res:
                        self.service_name = res[0][1].decode('utf8')
                        if debug:
                            print('discovered master', self.service_name)
                        break
                if not self.service_name:
                    raise SentinelError(
                        'no master detected, please specify service_name')
            new_nodes = set()
            for node in self.sentinels:
                res = node.runcmd('sentinel', 'sentinels', self.service_name)
                for _val in res:
                    val = [v.decode('utf8') for v in _val]
                    dv = dict(zip(val[::2], val[1::2]))
                    if not any((v.host == dv['ip']) and (v.port == int(dv['port']))
                               for v in self.sentinels):
                        new_nodes.add((dv['ip'], int(dv['port'])))
            for host, port in new_nodes:
                self.sentinels.append(
                    Node(host, port, 0, None, timeout or DEFAULT_SENTINEL_TIMEOUT)
                    )
                if self.debug:
                    print('discovered sentinel %s %d' % (host, port))
            self.node = None
        else:
            self.node = Node(self.host, self.port, self.db, self.password, self.timeout)
        self.transaction = False
        self.subscribed = False

    def __node__(self):
        if self.node is None:
            if self.sentinels:
                empty_master_result = False
                for node in self.sentinels:
                    try:
                        res = node.runcmd('sentinel','get-master-addr-by-name', self.service_name)
                        if (type(res) is list) and len(res) == 2:
                            bhost, bport = res
                            self.host = bhost.decode('utf8')
                            self.port = int(bport)
                            self.node = Node(
                                self.host, self.port, self.db, self.password, self.timeout)
                            break
                        else:
                            empty_master_result = True
                    except NodeError:
                        continue
                if self.node is None:
                    if empty_master_result:
                        raise SentinelErrorNoMaster('unable to get master from a sentinel')
                    else:
                        raise SentinelError('unable to connect to any sentinel')
            else:
                self.node = Node(
                    self.host, self.port, self.db, self.password, self.timeout)
        return self.node

    def listen(self, todict=False):
        while self.subscribed:
            r = self.__node__().parse_resp()
            # the message type comes back as bytes from parse_resp
            msgtype = r[0].decode("utf-8") if isinstance(r[0], bytes) else r[0]
            if msgtype == 'unsubscribe' and r[2] == 0:
                self.subscribed = False
            if todict:
                if msgtype == "pmessage":
                    r = dict(type=msgtype, pattern=r[1], channel=r[2], data=r[3])
                else:
                    r = dict(type=msgtype, pattern=None, channel=r[1], data=r[2])
            yield r

    def runcmd(self, cmdname, *args):
        # cluster not implemented
        if cmdname in ["MULTI", "WATCH"]:
            self.transaction = True
        if self.safe and not self.transaction and not self.subscribed:
            try:
                return self.__node__().runcmd(cmdname, *args)
            except NodeError:
                if self.sentinels:
                    self.node = None
                else:
                    time.sleep(self.safewait)

        if cmdname in ["DISCARD", "EXEC", "UNWATCH"]:
            self.transaction = False
        try:
            if cmdname in ["SUBSCRIBE", "PSUBSCRIBE",
                           "UNSUBSCRIBE", "PUNSUBSCRIBE"]:
                self.__node__().sendcmd(cmdname, *args)
                rsp = self.__node__().parse_resp()
            else:
                rsp = self.__node__().runcmd(cmdname, *args)
            if cmdname in ["SUBSCRIBE", "PSUBSCRIBE"]:
                self.subscribed = True
            return rsp
        except NodeError as e:
            if self.sentinels:
                self.node = None
            self.transaction = False
            self.subscribed = False
            raise

    def pipeline(self):
        self.multi()
        return self

    def _select(self, cmdname, *args):
        resp = self.runcmd(cmdname, *args)
        # parse_resp returns simple strings as bytes, so accept both forms
        if resp in ("OK", b"OK"):
            self.db = int(args[0])
        return resp

    def runcmdon(self, node, cmdname, *args):
        return self.node.runcmd(cmdname, *args)

    # -- version-aware command introspection ------------------------------

    def server_version(self):
        """Return the connected server's version as a tuple, e.g. (8, 0, 2),
        parsed from the ``redis_version`` field of ``INFO``."""
        info = self.info()
        if isinstance(info, bytes):
            info = info.decode("utf-8", "replace")
        for line in info.splitlines():
            if line.startswith("redis_version:"):
                return parse_version(line.split(":", 1)[1].strip())
        return None

    @staticmethod
    def command_json(command):
        """Return the upstream metadata dict for a command, accepting either
        the Redis name (``"GET"``, ``"CONFIG GET"``) or the Python method name
        (``"delete"``). Returns None if the command is unknown."""
        key = command.upper()
        if key in redisCommands:
            return redisCommands[key]
        method = command.lower()
        for name, meta in redisCommands.items():
            if command_method_name(name) == method:
                return meta
        return None

    @classmethod
    def supports(cls, command, version):
        """True if ``command`` exists at the given server ``version`` (a tuple
        or version string). Commands with no ``since`` are assumed available;
        commands absent from the description file are considered unsupported."""
        if isinstance(version, str):
            version = parse_version(version)
        meta = cls.command_json(command)
        if meta is None:
            return False
        since = meta.get("since")
        return since is None or parse_version(since) <= version

    @classmethod
    def commands_by_availability(cls, version):
        """Split every known command method name into ``(available,
        unsupported)`` lists for the given ``version`` (tuple or string)."""
        if isinstance(version, str):
            version = parse_version(version)
        available, unsupported = [], []
        for name, meta in redisCommands.items():
            method = command_method_name(name)
            since = meta.get("since")
            if since is None or parse_version(since) <= version:
                available.append(method)
            else:
                unsupported.append(method)
        return sorted(available), sorted(unsupported)

    def available_commands(self, version=None):
        """Sorted list of command method names available at ``version``
        (defaults to the connected server's version)."""
        if version is None:
            version = self.server_version()
        return self.commands_by_availability(version)[0]

    def unsupported_commands(self, version=None):
        """Sorted list of command method names NOT available at ``version``
        (defaults to the connected server's version)."""
        if version is None:
            version = self.server_version()
        return self.commands_by_availability(version)[1]


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

    def __connected__(self):
        return bool(self._sock)
    
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
            if self._sock is None:
                raise NodeError("Unable to connect")
            if self.password:
                if not self.runcmd("auth", self.password):
                    raise RedisError("Authentication error: Invalid password")
            if self._sock:
                if self.db:
                    self.runcmd("select", str(self.db))

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
            self.disconnect()
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
            self._sock.sendall(message+b"\r\n")
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
            # an empty read means the peer closed the connection
            self.disconnect()
            raise NodeError('Empty response')
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
        self.daemon = True
        self.channel = channel
        self.callback = callback
        self.param = redis_param
        self.start()

    def run(self):
        self._redis = Redis(**self.param)
        self._redis.subscribe(self.channel)
        for v in self._redis.listen():
            self.callback(v)
