=========================
Desir Redis Python Client
=========================

A small, **pythonic** Redis client.

The whole point of *desir* is to make Redis feel like Python rather than like a
wire protocol:

* **Every Redis command is a real method** — generated at import time from the
  official ``commands.json`` description file. They carry docstrings, so
  ``help(r.get)`` and tab-completion just work, and new Redis commands appear
  automatically when the description file is refreshed.
* **Redis types are wrapped as native Python idioms** — a counter you can
  iterate, compare and do arithmetic with (``c += 5``), a hash that behaves
  like both an object (``h.name``) and a mapping (``h["name"]``, ``"name" in
  h``, ``len(h)``), a string exposed as a descriptor, and a message
  ``Connector`` offering Erlang-style ``send`` / ``receive`` plus transparent
  remote-procedure calls through proxy objects.
* **Context managers for every resource** — connections, transactions,
  distributed locks, and pub/sub subscriptions all support the ``with``
  statement so resources are never left open accidentally.
* **Minimalist** — a thin, dependency-free core (pure standard library).

*Desir* is a permutation of *Redis*, and a nod to the desire of antirez for
6379 (aka MERZ): http://antirez.com/print.php?postid=220

.. contents::
   :local:


Install
=======

::

    python setup.py install

Requires Python 3. (A legacy Python 2 module, ``desir/desir.py``, is kept for
historical reasons but is no longer maintained.)


Quick start
===========

.. code-block:: python

    >>> import desir
    >>> r = desir.Redis()                 # host="localhost", port=6379, db=0

    >>> r.set("name", "Alice")
    b'OK'
    >>> r.get("name")
    b'Alice'
    >>> r.incr("hits")
    1
    >>> r.rpush("fruits", "apple", "pear", "fig")
    3
    >>> r.lrange("fruits", 0, -1)
    [b'apple', b'pear', b'fig']
    >>> r.type("fruits")
    b'list'
    >>> r.keys("*")
    [b'fruits', b'hits', b'name']

Replies come back as raw ``bytes`` (or ``int`` / ``list`` / ``None`` as
appropriate); decode them yourself with ``.decode()`` when you want text. Redis
errors are raised as ``desir.RedisError``:

.. code-block:: python

    >>> r.incr("name")            # "name" holds a string, not a counter
    Traceback (most recent call last):
        ...
    desir.RedisError: ERR value is not an integer or out of range

``DEL`` and ``EXEC`` are Python keywords, so they are exposed as ``delete()``
and ``execute()``; every other command keeps its Redis name, lower-cased.


Self-documenting commands
==========================

Because methods are generated from ``commands.json``, the documentation lives
right on them:

.. code-block:: python

    >>> help(r.rpop)
    Help on method rpop in module desir.desir3:

    rpop(self, *args) method of desir.desir3.Redis instance
        Returns and removes the last elements of a list. Deletes the list if the last element was popped.
        Parameters:
        Name: key,      Type: key,      Multiple parameter:False
        Name: count,    Type: integer,  Multiple parameter:False


Version-aware commands
======================

``commands.json`` records the Redis version each command was introduced in (its
``since`` field). *desir* exposes that, so you can tell which commands a given
server actually supports.

.. code-block:: python

    >>> r.server_version()                 # parsed from INFO
    (8, 0, 2)

    >>> r.supports("getdel", "6.0.0")
    False
    >>> r.supports("getdel", "6.2.0")      # GETDEL landed in 6.2.0
    True

    >>> available, unsupported = r.commands_by_availability("5.0.0")
    >>> len(available), len(unsupported)
    (281, 89)

The version argument is optional everywhere — leave it out and it defaults to
the connected server's version:

.. code-block:: python

    >>> r.supports("getdel")               # checked against the live server
    True
    >>> r.unsupported_commands()           # what this server can't do
    []

``supports`` accepts either the Redis name (``"GETDEL"``) or the Python method
name (``"getdel"``, ``"delete"``), and a version given as a string or a tuple.

To refresh the command set from the upstream description file:

.. code-block:: python

    >>> desir.reloadCommands(desir.COMMANDS_URL)

Matching the client to the server
----------------------------------

Pass ``match_version=True`` to bind the client to the connected server's
version. Commands the server is too old for are then disabled, so calling one
fails fast with a clear message instead of an opaque protocol error:

.. code-block:: python

    >>> r = desir.Redis(match_version=True)      # queries the server on connect
    >>> r.getdel("k")                            # against, say, a Redis 6.0 server
    Traceback (most recent call last):
        ...
    desir.RedisError: command 'getdel' requires Redis >= 6.2.0 but the server is 6.0.0 (client created with match_version=True)

It defaults to ``False`` (every known command is exposed).


Pythonic sugar
==============

These helpers are created from a ``Redis`` instance and share its connection.

Counter — an iterable, atomic counter
--------------------------------------

A ``Counter`` is a server-side atomic counter (backed by ``INCR``) that you can
iterate, share across processes/threads/hosts, and read with ``int()`` /
``str()``:

.. code-block:: python

    >>> r = desir.Redis()
    >>> c = r.Counter("hits", 0)           # key "hits", seeded at 0
    >>> next(c)
    1
    >>> next(c)
    2
    >>> int(c)
    2
    >>> for i in c:                        # an endless, distributed sequence
    ...     print(i)
    ...     if i >= 5:
    ...         break
    3
    4
    5

Pass ``seed=None`` to attach to an existing counter without resetting it.

Counters also support arithmetic and comparison operators:

.. code-block:: python

    >>> c = r.Counter("hits", seed=0)
    >>> c += 10                            # INCRBY 10
    >>> c -= 3                             # DECRBY 3
    >>> int(c)
    7
    >>> c == 7
    True
    >>> c > 5
    True
    >>> len(c)                             # same as int(c)
    7

Hash — a Redis hash as a Python object
--------------------------------------

A ``Hash`` maps attribute access to ``HGET`` / ``HSET`` and offers the usual
``keys`` / ``values`` / ``items``:

.. code-block:: python

    >>> user = r.Hash("user:1")
    >>> user.name = "Alice"
    >>> user.age = "30"
    >>> user.name
    b'Alice'
    >>> user.keys()
    [b'name', b'age']
    >>> list(user.items())
    [(b'name', b'Alice'), (b'age', b'30')]
    >>> user.missing                       # unknown field
    Traceback (most recent call last):
        ...
    AttributeError: Unkown attribute missing for object user:1

``Hash`` also implements the full mapping protocol — attribute access and item
access work side-by-side:

.. code-block:: python

    >>> user["email"] = "alice@example.com"   # HSET
    >>> user["email"]                          # HGET
    b'alice@example.com'
    >>> "email" in user                        # HEXISTS
    True
    >>> len(user)                              # HLEN
    3
    >>> del user["email"]                      # HDEL
    >>> "email" in user
    False

String — a Redis string as a descriptor
----------------------------------------

``String`` exposes a Redis key as a descriptor you can drop onto your own
classes:

.. code-block:: python

    >>> class Config:
    ...     title = r.String("site:title")
    ...
    >>> cfg = Config()
    >>> cfg.title = "Hello"                # -> SET site:title Hello
    >>> r.get("site:title")
    b'Hello'


Context managers
================

Every resource in desir supports the ``with`` statement.

Connection lifecycle
--------------------

Use ``Redis`` as a context manager to ensure the connection is closed when
you leave the block:

.. code-block:: python

    >>> with desir.Redis(host="redis-prod") as r:
    ...     r.set("key", "value")
    ...     r.get("key")
    b'value'
    # socket is disconnected on exit

Transactions
------------

``redis.transaction()`` wraps a block in ``MULTI`` / ``EXEC``.  If an
exception propagates out of the block the transaction is automatically
discarded (``DISCARD``) so it is never left open:

.. code-block:: python

    >>> with r.transaction():
    ...     r.set("balance", 100)
    ...     r.decrby("balance", 30)
    ...     r.incrby("reserve", 30)
    # all three commands are sent as one atomic EXEC

    >>> try:
    ...     with r.transaction():
    ...         r.set("x", 1)
    ...         raise RuntimeError("abort!")
    ... except RuntimeError:
    ...     pass
    # DISCARD was called; r.get("x") is whatever it was before

Distributed locks
-----------------

``redis.lock(name, ttl=30)`` acquires a server-side lock using
``SET … NX EX``.  The lock is released on exit only if the token still
matches (safe against expiry races where a second holder already took
over).  ``LockError`` is raised immediately if the lock is already held:

.. code-block:: python

    >>> with r.lock("payment:user:42", ttl=30):
    ...     process_payment()
    # DEL is called on clean exit; also released on exception

    >>> with r.lock("job"):
    ...     with r.lock("job"):       # same key — already held
    ...         pass
    desir.LockError: Could not acquire lock: 'job'

Subscriptions
-------------

``redis.subscription(*channels)`` subscribes on entry and automatically
calls ``UNSUBSCRIBE`` on exit, whether the block ends normally or via an
exception:

.. code-block:: python

    >>> with r.subscription("news", "sports") as messages:
    ...     for msg in messages:
    ...         handle(msg)
    # UNSUBSCRIBE "news" "sports" called automatically


Message passing with Connector
===============================

A ``Connector`` is an Erlang-style mailbox: each one is named, and that name is
a Redis list used as its inbox. Any JSON-serializable object can be sent.

.. code-block:: python

    # process A
    >>> r = desir.Redis()
    >>> a = r.Connector("alice")

    # process B
    >>> r = desir.Redis()
    >>> b = r.Connector("bob")
    >>> a.send("bob", {"hello": "bob"})

    # back in process B — a Connector is iterable
    >>> b.receive(timeout=5).val
    {'hello': 'bob'}

Each delivered message is an ``SWM`` (a dict with attribute access) carrying
``src`` / ``dst`` / ``time`` / ``val``:

.. code-block:: python

    >>> msg = b.receive(timeout=5)
    >>> msg.src, msg.val
    ('alice', {'hello': 'bob'})

A connector is also an iterator; when given a ``timeout`` it stops once the
inbox stays empty that long:

.. code-block:: python

    >>> b = r.Connector("bob", timeout=5)
    >>> for msg in b:                      # loops until 5s pass with no message
    ...     print(msg.src, msg.val)

Useful options:

* ``timeout`` — blocking time for ``receive`` / iteration (``0`` blocks
  forever, ``-1`` is non-blocking).
* ``fifo`` — FIFO (default) or LIFO ordering.
* ``safe`` — pop messages onto a per-consumer in-flight list so an
  interrupted consumer can ``release`` / ``unreceive`` them (at-least-once
  delivery).
* ``secret`` — sign every message with HMAC-SHA256; tampered or mis-keyed
  messages raise ``ConnectorError`` on receipt.

.. code-block:: python

    >>> a = r.Connector("alice", secret=b"shared-key")
    >>> b = r.Connector("bob",   secret=b"shared-key")
    >>> a.send("bob", "authenticated payload")


Remote procedure calls
=======================

Register functions on a connector, run it as a worker, and call those functions
from anywhere as if they were local — the proxy turns attribute access into a
request/reply round-trip.

The worker:

.. code-block:: python

    >>> import desir
    >>> conn = desir.Redis().Connector("calc")
    >>> @conn.register
    ... def add(*args):
    ...     return sum(args)
    ...
    >>> conn.worker()                      # blocks; run in its own process/thread

The client:

.. code-block:: python

    >>> proxy = desir.Redis().Connector("client", timeout=5).proxy("calc")
    >>> proxy.add(10, 20, 30, 40, 50)
    150
    >>> dir(proxy)                         # discover what the worker exposes
    ['add']

Exceptions raised on the worker propagate back as ``ConnectorError`` (and the
worker keeps serving):

.. code-block:: python

    >>> proxy.add(1, "not-a-number")
    Traceback (most recent call last):
        ...
    desir.ConnectorError: Error on worker side: TypeError("unsupported operand ...")


Pub/Sub
=======

Subscribe and consume messages by iterating ``listen()``:

.. code-block:: python

    >>> r = desir.Redis()
    >>> r.subscribe("news")
    [b'subscribe', b'news', 1]
    >>> for message in r.listen():         # blocks, yielding each message
    ...     print(message)
    [b'message', b'news', b'hello']

Or hand a channel and a callback to ``SubAsync`` to receive messages on a
background thread:

.. code-block:: python

    >>> def on_message(m):
    ...     print("received", m)
    ...
    >>> desir.SubAsync("news", on_message)
    >>> # meanwhile, from anywhere:  r.publish("news", "hello")
    received [b'message', b'news', b'hello']


Connecting
==========

.. code-block:: python

    # explicit connection parameters
    r = desir.Redis(host="localhost", port=6379, db=0,
                    password="secret", timeout=5)

    # safe mode: transparently retry/reconnect around dropped connections
    r = desir.Redis(safe=True)

    # high availability via Redis Sentinel — master and peer sentinels are
    # auto-discovered; pass service_name to skip master auto-detection
    r = desir.Redis(sentinels=[("10.0.0.1", 26379), ("10.0.0.2", 26379)],
                    service_name="mymaster")

You can also drive the pythonic objects with redis-py instead of the built-in
transport by pointing ``Connector._redis`` (etc.) at a redis-py client.


Testing
=======

The test-suite lives in ``tests/`` and runs with pytest::

    pip install pytest
    python -m pytest tests/

Pure-logic tests (command metaprogramming, version filtering, the ``SWM``
message object and the HMAC helper) always run. Integration tests need a
reachable Redis server; they default to ``localhost:6379`` database ``9``
(assumed disposable) and are skipped automatically when no server is
available. Override the target with the ``DESIR_TEST_HOST`` /
``DESIR_TEST_PORT`` / ``DESIR_TEST_DB`` environment variables.


License
=======

New BSD License. See the headers in the source files.
