
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

try:
    import simplejson as json
except ImportError:
    import json
import time
from uuid import uuid4
import os


class ConnectorError(Exception):
    pass


class SWM(dict):
    """ Connector message """

    def __getattr__(self, item):
        try:
            d = self.__getitem__(item)
        except KeyError:
            raise(AttributeError)
        # if value is the only key in object, you can omit it
        # this section needs to be documented
        # as i can't remember what it is used for.
        if isinstance(d, dict) and 'value' in d and len(d) == 1:
            return d['value']
        else:
            return d

    def __setattr__(self, item, value):
        self.__setitem__(item, value)


class ConnectorProxy(object):
    def __init__(self, connector, remotename):
        self.connector = connector
        self.remotename = remotename

    def __getattr__(self, item):
        def func(*args, **kwargs):
            return self.connector.run(self.remotename,
                                      item,
                                      *args, **kwargs)
        return func


class Counter:
    def __init__(self, name, seed=0):
        self.name = name
        if seed is not None:
            self._redis.set(self.name, seed)

    def __iter__(self):
        return self

    def __int__(self):
        return int(self._redis.get(self.name))

    def __str__(self):
        return self._redis.get(self.name)

    def __next__(self):
        return self._redis.incr(self.name)


class String(object):
    """
    Redis String descriptor object
    """
    def __init__(self, name):
        self.name = name

    def __get__(self, instance, owner):

        return [self, instance, owner, self._redis.get(self.name)]

    def __set__(self, instance, value):
        return self._redis.set(self.name, value)


class Connector(object):
    """
    safe queue implementation under work
    CONNECTORNAME:PID:TIMESTAMP
    """
    def __init__(self, name=None, ctype="", timeout=0, fifo=True,
                 safe=False, secret=None, serializer=json):
        if name is None:
            self.name = str(uuid4())
        else:
            self.name = name
        self.timeout = timeout
        # connector queue/list set to fifo when fifo is true, lifo otherwise
        self.fifo = fifo
        # when safe is true, connector works in safe mode meaning each time
        # a value is popped out of the list it is atomically pushed
        # to a dedicated list
        # when the program is done processing the object that was popped out,
        # it can release it with the release commands which will remove it
        # from the dedicated list
        self.safe = safe
        self.ctype = ctype
        self.secret = secret
        self.serializer = serializer
        self.pipeline = None
        self.callback = {}

    def register(self, func):
        def wrapper(*args, **kwargs):
            func(*args, **kwargs)
        self.callback[func.__name__] = func
        return wrapper

    @property
    def redis(self):
        if self.pipeline is None:
            return self._redis
        else:
            return self.pipeline

    def __iter__(self):
        return self

    def sendreceive(self, name, val=None, timeout=0, funcname=None):
        srcreply = "%s:%s:%s" % (self.name, str(time.time()), str(uuid4()))
        self.send(name, val, srcreply, funcname=funcname)
        return self.receive(timeout=timeout, srcreply=srcreply)

    def send(self, name, val=None, srcreply=None, funcname=None,
             exception=False):
        if srcreply:
            vd = SWM(src=srcreply, srctype=self.ctype,
                     dst=name, time=time.time(), val=val)
        else:
            vd = SWM(src=self.name, srctype=self.ctype,
                     dst=name, time=time.time(), val=val)
        # can't remember why this section was commented...
        # if not val:
        #    vd.update(name)
        if funcname:
            vd.funcname = funcname
        if exception:
            vd.exception = True
        vp = self.serializer.dumps(vd)
        if self.secret:
            import hashlib
            vs = hashlib.sha1()
            vs.update(vp)
            vs.update(self.secret)
            vp = vs.digest() + vp
        if self.fifo:
            return self.redis.lpush(vd.dst, vp)
        else:
            return self.redis.rpush(vd.dst, vp)

    def receive(self, timeout=0, srcreply=None):
        tmpname = "%s:%d:%d" % (self.name, os.getpid(), int(time.time()))
        if srcreply is None:
            srcreply = self.name
        if self.safe:
            if timeout == -1:
                resp = self.redis.rpoplpush(srcreply, tmpname)
            else:
                resp = self.redis.brpoplpush(srcreply, tmpname, timeout)
        else:
            if timeout == -1:
                resp = self.redis.rpop(srcreply)
            else:
                resp = self.redis.brpop(srcreply, timeout)
                resp = resp and resp[1]
        if resp:
            if self.secret:
                import hashlib
                vs = hashlib.sha1()
                vs.update(resp[20:])
                vs.update(self.secret)
                if resp[:20] != vs.digest():
                    raise ConnectorError("Digest signature failed")
                resp = resp[20:]
            resp = self.serializer.loads(resp)
            if type(resp) is dict:
                resp = SWM(resp)
            if self.safe:
                resp["srcack"] = tmpname
        return resp

    def unreceive(self, val):
        if "srcack" in val:
            return self._redis.rpoplpush(val.srcack, self.name)

    def transfer(self, name, val, newval, force=True):
        res = None
        while not res:
            self.redis.watch(val.srcack)
            self.pipeline = self.redis.pipeline()
            self.release(val)
            self.send(name, newval)
            res = self.pipeline.execute()
            self.pipeline = None
            if not force:
                break
        return res

    def reply(self, val, newval, force=True, exception=False):
        res = None
        while not res:
            if "srcack" in val:
                self.redis.watch(val.srcack)
            self.pipeline = self.redis.pipeline()
            self.release(val)
            self.send(val.src, newval, exception=exception)
            res = self.pipeline.execute()
            self.pipeline = None
            if not force:
                break
        return res

    def worker(self, is_running=lambda: True):
        while is_running():
            res = self.receive(timeout=1)
            if res:
                if res.funcname in self.callback:
                    try:
                        resr = self.callback[res.funcname](
                            *res.val.get("args", []),
                            **res.val.get("kwargs", {}))
                    except Exception as e:
                        self.reply(res, repr(e),
                                   exception=True)
                        raise
                    self.reply(res, resr)
                else:
                    self.reply(res,
                               "No such function name %s" % (res.funcname),
                               exception=True)

    def run(self, name, funcname, *args, **kwargs):
        val = dict(args=args, kwargs=kwargs)
        res = self.sendreceive(name, val=val,
                               timeout=self.timeout,
                               funcname=funcname)
        if res is None and self.timeout:
            raise ConnectorError("Timeout")
        if res.get("exception"):
            raise ConnectorError("Error on worker side: %s" % (res.val))
        return res.val

    def proxy(self, name):
        return ConnectorProxy(self, name)

    def release(self, val):
        if "srcack" in val:
            return self.pipeline.rpop(val.srcack)

    def __next__(self):
        resp = self.receive(self.timeout)
        if resp:
            return resp
        else:
            raise StopIteration


class Hash(object):
    def __init__(self, name):
        self._keyid = name

    def __repr__(self):
        return str(self.items())

    def __getattr__(self, item):
        if item.startswith("_"):
            return object.__getattribute__(self, item)
        resp = self._redis.hget(self._keyid, item)
        if resp:
            return resp
        else:
            raise AttributeError("Unkown attribute %s for object %s" % (
                item, self._keyid))

    def __setattr__(self, item, value):
        if item.startswith("_"):
            return object.__setattr__(self, item, value)
        else:
            self._redis.hset(self._keyid, item, value)

    def keys(self):
        return self._redis.hkeys(self._keyid)

    def values(self):
        return self._redis.hvals(self._keyid)

    def items(self):
        resp = self._redis.hgetall(self._keyid)
        if resp:
            return zip(resp[::2], resp[1::2])
