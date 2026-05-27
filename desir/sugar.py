
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
import hmac
import hashlib
from uuid import uuid4
import os

# length in bytes of the HMAC-SHA256 digest prepended to signed messages
_MAC_SIZE = 32


def _sign(secret, payload):
    """Return HMAC-SHA256(payload) using secret (bytes or str)."""
    if isinstance(secret, str):
        secret = secret.encode("utf-8")
    return hmac.new(secret, payload, hashlib.sha256).digest()


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

    def __dir__(self):
        return self.connector.run(self.remotename, '__dir__')


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
        val = self._redis.get(self.name)
        if isinstance(val, bytes):
            val = val.decode("utf-8")
        return str(val)

    def __next__(self):
        return self._redis.incr(self.name)

    def __iadd__(self, n):
        self._redis.incrby(self.name, n)
        return self

    def __isub__(self, n):
        self._redis.decrby(self.name, n)
        return self

    def __len__(self):
        return int(self)

    def __eq__(self, other):
        return int(self) == int(other)

    def __lt__(self, other):
        return int(self) < int(other)

    def __le__(self, other):
        return int(self) <= int(other)

    def __gt__(self, other):
        return int(self) > int(other)

    def __ge__(self, other):
        return int(self) >= int(other)


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
        # def wrapper(*args, **kwargs):
        # func(*args, **kwargs)
        self.callback[func.__name__] = func
        return func

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
        if isinstance(vp, str):
            vp = vp.encode("utf-8")
        if self.secret:
            vp = _sign(self.secret, vp) + vp
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
                mac, resp = resp[:_MAC_SIZE], resp[_MAC_SIZE:]
                if not hmac.compare_digest(mac, _sign(self.secret, resp)):
                    raise ConnectorError("Digest signature failed")
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
            self._redis.multi()
            self.pipeline = self._redis
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
            self._redis.multi()
            self.pipeline = self._redis
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
                        # report the failure to the caller but keep the
                        # worker loop alive for subsequent requests
                        self.reply(res, repr(e), exception=True)
                        continue
                    self.reply(res, resr)
                elif res.funcname == '__dir__':
                    resr = list(self.callback.keys())
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
        return str(list(self.items()))

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

    def __getitem__(self, key):
        return self._redis.hget(self._keyid, key)

    def __setitem__(self, key, value):
        self._redis.hset(self._keyid, key, value)

    def __delitem__(self, key):
        self._redis.hdel(self._keyid, key)

    def __contains__(self, key):
        return bool(self._redis.hexists(self._keyid, key))

    def __len__(self):
        return self._redis.hlen(self._keyid)

    def keys(self):
        return self._redis.hkeys(self._keyid)

    def values(self):
        return self._redis.hvals(self._keyid)

    def items(self):
        resp = self._redis.hgetall(self._keyid)
        if resp:
            return zip(resp[::2], resp[1::2])


class LockError(Exception):
    pass


class Lock:
    """Distributed lock backed by a Redis key.

    Acquired with ``SET key <token> NX EX ttl``.  Released only if the
    stored value still matches the token acquired on entry, preventing
    accidental release of a lock that expired and was re-acquired by
    another holder.

    Intended to be used as a context manager via ``redis.lock()``.
    """

    def __init__(self, name, ttl=30):
        self.name = name
        self.ttl = ttl
        self._token = None

    def __enter__(self):
        token = str(uuid4())
        ok = self._redis.set(self.name, token, "NX", "EX", self.ttl)
        if not ok:
            raise LockError("Could not acquire lock: %r" % self.name)
        self._token = token
        return self

    def __exit__(self, *_):
        if self._token is None:
            return
        current = self._redis.get(self.name)
        if current is not None:
            if isinstance(current, bytes):
                current = current.decode("utf-8")
            if current == self._token:
                self._redis.delete(self.name)
        self._token = None


class StreamGroup:
    """Consumer group tied to a Stream.

    Obtain via ``stream.group(name, consumer)`` rather than instantiating
    directly.  ``_redis`` is reached through the parent ``Stream`` instance so
    no ``RedisInner`` wrapping is required here.
    """

    def __init__(self, stream, group, consumer):
        self.stream = stream
        self.group = group
        self.consumer = consumer

    @property
    def _redis(self):
        return self.stream._redis

    def create(self, start="$", mkstream=False):
        """Create the consumer group on the server.

        *start* is the entry-ID from which the group will start consuming;
        use ``"0"`` to replay the whole stream or ``"$"`` (default) to receive
        only new entries.  Pass ``mkstream=True`` to have the stream key
        created automatically when it does not yet exist.
        """
        args = [self.stream.name, self.group, start]
        if mkstream:
            args.append("MKSTREAM")
        return self._redis.xgroup_create(*args)

    def destroy(self):
        """Delete this consumer group from the server."""
        return self._redis.xgroup_destroy(self.stream.name, self.group)

    def read(self, count=None, block=None):
        """Fetch undelivered entries for this consumer (``>``).

        *count* caps the number of entries returned per call.
        *block* is a millisecond timeout; ``0`` blocks indefinitely.
        Returns a list of ``(entry_id, fields_dict)`` tuples.
        """
        args = ["GROUP", self.group, self.consumer]
        if count is not None:
            args += ["COUNT", count]
        if block is not None:
            args += ["BLOCK", block]
        args += ["STREAMS", self.stream.name, ">"]
        raw = self._redis.xreadgroup(*args)
        if not raw:
            return []
        return [self.stream._parse_entry(e) for e in raw[0][1]]

    def ack(self, *entry_ids):
        """Acknowledge one or more processed entry IDs."""
        return self._redis.xack(self.stream.name, self.group, *entry_ids)

    def pending(self, count=10, start="-", end="+"):
        """Return pending (unacknowledged) entries for this group."""
        return self._redis.xpending(
            self.stream.name, self.group, start, end, count)

    def __iter__(self):
        """Drain all immediately available entries one at a time."""
        while True:
            entries = self.read(count=1)
            if not entries:
                return
            yield entries[0]


class Stream:
    """Pythonic wrapper around a Redis Stream key.

    Usage::

        s = redis.Stream("events")
        s << {"type": "login", "user": "alice"}   # append
        for entry_id, data in s:                   # iterate history
            print(entry_id, data)

        grp = s.group("workers", "consumer-1")
        grp.create(start="0", mkstream=True)
        for entry_id, data in grp:
            process(data)
            grp.ack(entry_id)
    """

    def __init__(self, name, maxlen=None):
        self.name = name
        self.maxlen = maxlen  # when set, every add() applies MAXLEN ~ N

    # --- write ----------------------------------------------------------

    def add(self, data, entry_id="*"):
        """Append *data* (a mapping) to the stream, returning the new entry ID."""
        args = [self.name]
        if self.maxlen is not None:
            args += ["MAXLEN", "~", self.maxlen]
        args.append(entry_id)
        for k, v in data.items():
            args += [k, v]
        return self._redis.xadd(*args)

    def __lshift__(self, data):
        """``stream << {"field": "value"}`` — append shorthand."""
        return self.add(data)

    def trim(self, maxlen, approximate=True):
        """Trim the stream to at most *maxlen* entries."""
        args = [self.name, "MAXLEN"]
        if approximate:
            args.append("~")
        args.append(maxlen)
        return self._redis.xtrim(*args)

    # --- parse ----------------------------------------------------------

    @staticmethod
    def _parse_entry(raw):
        """Convert ``[id_bytes, [f, v, f, v, …]]`` → ``(id_str, {f: v})``."""
        raw_id, raw_fields = raw
        entry_id = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
        it = iter(raw_fields)
        fields = {
            (k.decode() if isinstance(k, bytes) else k): v
            for k, v in zip(it, it)
        }
        return entry_id, fields

    # --- read -----------------------------------------------------------

    def range(self, start="-", end="+", count=None):
        """Return entries between *start* and *end* as ``(id, fields)`` tuples."""
        args = [self.name, start, end]
        if count is not None:
            args += ["COUNT", count]
        raw = self._redis.xrange(*args)
        return [self._parse_entry(e) for e in raw] if raw else []

    def revrange(self, end="+", start="-", count=None):
        """Like :meth:`range` but in reverse chronological order."""
        args = [self.name, end, start]
        if count is not None:
            args += ["COUNT", count]
        raw = self._redis.xrevrange(*args)
        return [self._parse_entry(e) for e in raw] if raw else []

    def read(self, count=None, last_id="0"):
        """Read entries newer than *last_id* (non-group XREAD)."""
        args = []
        if count is not None:
            args += ["COUNT", count]
        args += ["STREAMS", self.name, last_id]
        raw = self._redis.xread(*args)
        if not raw:
            return []
        return [self._parse_entry(e) for e in raw[0][1]]

    # --- collection protocol --------------------------------------------

    def __len__(self):
        return self._redis.xlen(self.name)

    def __iter__(self):
        return iter(self.range())

    # --- introspection --------------------------------------------------

    def info(self):
        """Return the XINFO STREAM metadata dict for this stream."""
        return self._redis.xinfo_stream(self.name)

    # --- consumer groups ------------------------------------------------

    def group(self, name, consumer):
        """Return a :class:`StreamGroup` for *name* and *consumer*."""
        return StreamGroup(self, name, consumer)
