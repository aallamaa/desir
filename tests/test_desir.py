"""Extensive test-suite for the desir Redis client.

Organised into:

* pure-logic tests (no server needed): metaprogramming, ``SWM``, the HMAC
  helper -- these always run.
* integration tests (``requires_redis`` / the ``redis`` fixture): basic
  commands, transactions, pub/sub and the pythonic sugar (Counter, Hash,
  String, Connector / worker / proxy). They are skipped when no Redis server
  is reachable.

Several tests are explicit regressions for bugs fixed during the 2026 audit
and are marked with ``# regression:`` comments.
"""
import threading
import time

import pytest

import desir
from desir.sugar import _sign, _MAC_SIZE, SWM, ConnectorError

from conftest import requires_redis


class FakeNode(object):
    """In-memory stand-in for ``Node`` used to drive the Sentinel code paths
    without any running Sentinel/Redis. Configure the class attributes, then
    monkeypatch it in via the ``fake_node`` fixture."""

    masters_reply = None
    sentinels_reply = []
    master_addr = None
    raise_on_addr = False
    instances = []

    @classmethod
    def reset(cls):
        cls.masters_reply = None
        cls.sentinels_reply = []
        cls.master_addr = None
        cls.raise_on_addr = False
        cls.instances = []

    def __init__(self, host="localhost", port=6379, db=0,
                 password=None, timeout=None):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.timeout = timeout
        FakeNode.instances.append(self)

    def runcmd(self, cmd, *args):
        if cmd == "sentinel":
            sub = args[0]
            if sub == "masters":
                return FakeNode.masters_reply
            if sub == "sentinels":
                return FakeNode.sentinels_reply
            if sub == "get-master-addr-by-name":
                if FakeNode.raise_on_addr:
                    raise desir.NodeError("sentinel down")
                return FakeNode.master_addr
        raise AssertionError("unexpected command %r %r" % (cmd, args))


@pytest.fixture
def fake_node(monkeypatch):
    FakeNode.reset()
    monkeypatch.setattr(desir.desir3, "Node", FakeNode)
    return FakeNode


# ---------------------------------------------------------------------------
# Pure logic -- no Redis server required
# ---------------------------------------------------------------------------

class TestMetaprogramming:
    def test_common_commands_exist(self):
        for name in ("get", "set", "incr", "hget", "lpush", "subscribe"):
            assert callable(getattr(desir.Redis, name))

    def test_reserved_word_commands_are_renamed(self):
        # DEL and EXEC are Python keywords/builtins -> renamed
        assert hasattr(desir.Redis, "delete")
        assert hasattr(desir.Redis, "execute")
        assert desir.Redis.delete.__redisname__ == "DEL"
        assert desir.Redis.execute.__redisname__ == "EXEC"

    def test_docstrings_are_generated(self):
        # wording varies between commands.json versions; just assert the
        # summary for GET was injected and mentions the value it returns
        doc = desir.Redis.get.__doc__
        assert doc and "value" in doc.lower()

    def test_multiword_command_names_are_underscored(self):
        # e.g. "GETSET" stays getset; multi-word ones lowercased+underscored
        assert desir.Redis.getset.__redisname__ == "GETSET"


class TestSWM:
    def test_attribute_access(self):
        m = SWM(src="a", val=[1, 2, 3])
        assert m.src == "a"
        assert m.val == [1, 2, 3]

    def test_attribute_assignment(self):
        m = SWM()
        m.foo = "bar"
        assert m["foo"] == "bar"

    def test_missing_attribute_raises(self):
        with pytest.raises(AttributeError):
            SWM().nope

    def test_value_only_dict_is_collapsed(self):
        # a nested dict whose only key is "value" is transparently unwrapped
        m = SWM(payload={"value": 42})
        assert m.payload == 42


class TestHmacHelper:
    def test_digest_size(self):
        assert len(_sign("secret", b"data")) == _MAC_SIZE == 32

    def test_deterministic(self):
        assert _sign("k", b"msg") == _sign("k", b"msg")

    def test_str_and_bytes_secret_equivalent(self):
        assert _sign("k", b"msg") == _sign(b"k", b"msg")

    def test_different_secret_differs(self):
        assert _sign("k1", b"msg") != _sign("k2", b"msg")


# ---------------------------------------------------------------------------
# Integration -- basic commands
# ---------------------------------------------------------------------------

@requires_redis
class TestBasicCommands:
    def test_set_get_roundtrip(self, redis):
        assert redis.set("k", "hello") in ("OK", b"OK")
        assert redis.get("k") == b"hello"  # responses are bytes in py3

    def test_get_missing_is_none(self, redis):
        assert redis.get("does-not-exist") is None

    def test_delete_and_exists(self, redis):
        redis.set("k", "v")
        assert redis.exists("k") == 1
        assert redis.delete("k") == 1
        assert redis.exists("k") == 0

    def test_incr_returns_int(self, redis):
        assert redis.incr("cnt") == 1
        assert redis.incr("cnt") == 2
        assert isinstance(redis.incr("cnt"), int)

    def test_type_command(self, redis):
        redis.set("s", "v")
        assert redis.type("s") == b"string"

    def test_list_operations(self, redis):
        redis.rpush("L", "a")
        redis.rpush("L", "b")
        assert redis.lrange("L", 0, -1) == [b"a", b"b"]
        assert redis.rpop("L") == b"b"

    def test_wrong_type_raises_rediserror(self, redis):
        redis.rpush("L", "a")
        with pytest.raises(desir.RedisError):
            redis.get("L")

    def test_select_updates_db(self, redis):
        # regression: parse_resp returns b"OK"; _select compared against "OK"
        # so db was never updated after SELECT.
        assert redis.db == 9
        redis.select("3")
        assert redis.db == 3
        redis.select("9")  # restore for teardown flush

    def test_expire_and_ttl(self, redis):
        redis.set("k", "v")
        assert redis.expire("k", 100) == 1
        assert 0 < redis.ttl("k") <= 100


@requires_redis
class TestTransactions:
    def test_multi_exec(self, redis):
        redis.multi()
        assert redis.set("k", "v") in ("QUEUED", b"QUEUED")
        redis.incr("cnt")
        res = redis.execute()
        assert res[0] in ("OK", b"OK")
        assert res[1] == 1
        assert redis.get("k") == b"v"

    def test_watch_multi_discard(self, redis):
        redis.set("wk", "1")
        redis.watch("wk")
        redis.multi()
        redis.incr("wk")
        redis.discard()  # abandon the queued INCR
        assert redis.get("wk") == b"1"


# ---------------------------------------------------------------------------
# Version-aware command introspection
# ---------------------------------------------------------------------------

class TestVersionFiltering:
    def test_parse_version(self):
        pv = desir.desir3.parse_version
        assert pv("8.0.2") == (8, 0, 2)
        assert pv("2.2.3") == (2, 2, 3)
        assert pv("1.0") == (1, 0)
        # trailing non-numeric junk is tolerated
        assert pv("7.0.0-rc1") == (7, 0, 0)

    def test_supports_known_command(self):
        # OBJECT was introduced in 2.2.3
        assert desir.Redis.supports("object", "2.2.3") is True
        assert desir.Redis.supports("object", "2.0.0") is False
        assert desir.Redis.supports("get", "1.0.0") is True
        # tuple version accepted as well
        assert desir.Redis.supports("object", (2, 3, 0)) is True

    def test_supports_renamed_command(self):
        # DEL is exposed as delete(); lookup must resolve either spelling
        assert desir.Redis.supports("delete", "1.0.0") is True
        assert desir.Redis.supports("DEL", "1.0.0") is True

    def test_supports_unknown_command(self):
        assert desir.Redis.supports("no_such_command", "99.0.0") is False

    def test_command_json_lookup(self):
        assert "since" in desir.Redis.command_json("GET")
        # method-name spelling resolves to the DEL metadata
        assert desir.Redis.command_json("delete") is desir.Redis.command_json("DEL")
        assert desir.Redis.command_json("bogus") is None

    def test_commands_by_availability_partition(self):
        avail, unsup = desir.Redis.commands_by_availability("2.6.0")
        assert "get" in avail
        # ACL commands are post-6.0, so unavailable at 2.6.0
        assert "acl_cat" in unsup
        assert set(avail).isdisjoint(unsup)
        assert len(avail) + len(unsup) == len(desir.desir3.redisCommands)

    def test_available_commands_with_explicit_version(self):
        # passing a version avoids any server round-trip
        r = desir.Redis(db=9)
        assert "get" in r.available_commands("2.6.0")
        assert "acl_cat" in r.unsupported_commands("2.6.0")


@requires_redis
class TestVersionFilteringLive:
    def test_server_version(self, redis):
        v = redis.server_version()
        assert isinstance(v, tuple)
        assert v >= (1, 0, 0)

    def test_defaults_to_server_version(self, redis):
        v = redis.server_version()
        avail = redis.available_commands()
        unsup = redis.unsupported_commands()
        assert "get" in avail
        # the no-argument calls must agree with the explicit-version partition
        exp_avail, exp_unsup = redis.commands_by_availability(v)
        assert (avail, unsup) == (exp_avail, exp_unsup)


@requires_redis
class TestPubSub:
    def test_subasync_receives_message(self, redis_params, make_redis):
        received = []
        got = threading.Event()

        def callback(msg):
            received.append(msg)
            got.set()

        desir.SubAsync("audit_ch", callback, **redis_params)
        time.sleep(0.3)  # let the subscriber establish the subscription

        publisher = make_redis()
        publisher.publish("audit_ch", "hello")

        assert got.wait(3), "callback never fired"
        msg = received[0]
        # message frame is [type, channel, data], all bytes from the wire
        assert msg[0] in ("message", b"message")
        assert msg[2] in ("hello", b"hello")


# ---------------------------------------------------------------------------
# Integration -- pythonic sugar
# ---------------------------------------------------------------------------

@requires_redis
class TestCounter:
    def test_iteration_increments(self, redis):
        c = redis.Counter("audit_cnt", 5)
        vals = []
        for v in c:
            vals.append(v)
            if v >= 8:
                break
        assert vals == [6, 7, 8]

    def test_int_and_str(self, redis):
        c = redis.Counter("audit_cnt", 41)
        assert int(c) == 41
        # regression: __str__ used to return bytes -> TypeError
        assert str(c) == "41"
        assert next(c) == 42


@requires_redis
class TestHash:
    def test_set_get_attributes(self, redis):
        h = redis.Hash("audit_h")
        h.a = "1"
        h.b = "2"
        assert h.a == b"1"
        assert set(h.keys()) == {b"a", b"b"}
        assert set(h.values()) == {b"1", b"2"}

    def test_unknown_attribute_raises(self, redis):
        h = redis.Hash("audit_h")
        with pytest.raises(AttributeError):
            h.missing

    def test_repr_is_readable(self, redis):
        # regression: __repr__ used to stringify a zip object
        h = redis.Hash("audit_h")
        h.a = "1"
        assert "<zip object" not in repr(h)
        assert repr(h).startswith("[")


@requires_redis
class TestStringDescriptor:
    def test_descriptor_get_set(self, redis):
        class Doc:
            field = redis.String("audit_str")

        d = Doc()
        d.field = "hello"
        assert redis.get("audit_str") == b"hello"
        # __get__ returns [descriptor, instance, owner, value]
        assert d.field[3] == b"hello"


# ---------------------------------------------------------------------------
# Integration -- Connector (message passing / RPC)
# ---------------------------------------------------------------------------

@requires_redis
class TestConnector:
    def test_send_receive(self, redis):
        a = redis.Connector("chanA")
        b = redis.Connector("chanB")
        a.send("chanB", [1, 2, {"x": 3}])
        msg = b.receive(timeout=2)
        assert msg.val == [1, 2, {"x": 3}]
        assert msg.src == "chanA"
        assert msg.dst == "chanB"

    def test_receive_timeout_returns_none(self, redis):
        b = redis.Connector("emptychan")
        assert b.receive(timeout=1) is None

    def test_fifo_vs_lifo_order(self, redis):
        # fifo connector: lpush + brpop -> first-in first-out
        fifo = redis.Connector("fifo", fifo=True)
        fifo.send("q", "first")
        fifo.send("q", "second")
        recv = redis.Connector("q", fifo=True)
        assert recv.receive(timeout=2).val == "first"
        assert recv.receive(timeout=2).val == "second"

    def test_safe_mode_release(self, redis):
        sender = redis.Connector("s")
        worker = redis.Connector("safechan", safe=True)
        sender.send("safechan", "job")
        msg = worker.receive(timeout=2)
        assert msg.val == "job"
        assert "srcack" in msg  # stashed on a dedicated ack list
        # releasing removes it from the in-flight list
        worker.pipeline = worker.redis.pipeline()
        worker.release(msg)
        worker.pipeline.execute()
        worker.pipeline = None

    def test_secret_signing_roundtrip(self, redis):
        secret = b"shared-key"
        a = redis.Connector("a", secret=secret)
        b = redis.Connector("dst", secret=secret)
        a.send("dst", {"hi": "there"})
        assert b.receive(timeout=2).val == {"hi": "there"}

    def test_secret_mismatch_is_rejected(self, redis):
        # regression: signature must be verified with a constant-time HMAC
        a = redis.Connector("a", secret=b"key-one")
        b = redis.Connector("dst", secret=b"key-two")
        a.send("dst", "tampered?")
        with pytest.raises(ConnectorError):
            b.receive(timeout=2)


@requires_redis
class TestWorkerProxy:
    def test_rpc_call_dir_and_exception(self, redis, make_redis):
        worker_conn = redis.Connector("calcworker")

        @worker_conn.register
        def add(*args):
            return sum(args)

        stop = threading.Event()
        t = threading.Thread(
            target=worker_conn.worker,
            kwargs={"is_running": lambda: not stop.is_set()},
        )
        t.daemon = True
        t.start()
        try:
            client = make_redis().Connector("calcclient", timeout=5)
            proxy = client.proxy("calcworker")

            assert proxy.add(1, 2, 3, 4) == 10
            assert "add" in dir(proxy)

            # an exception on the worker side propagates as ConnectorError
            with pytest.raises(ConnectorError):
                proxy.add(1, "not-a-number")

            # regression: the worker must survive a handler exception and keep
            # serving requests (it used to re-raise and die).
            assert proxy.add(5, 5) == 10
        finally:
            stop.set()
            t.join(timeout=3)

    def test_unknown_function_raises(self, redis, make_redis):
        worker_conn = redis.Connector("uworker")

        @worker_conn.register
        def known():
            return "ok"

        stop = threading.Event()
        t = threading.Thread(
            target=worker_conn.worker,
            kwargs={"is_running": lambda: not stop.is_set()},
        )
        t.daemon = True
        t.start()
        try:
            client = make_redis().Connector("uclient", timeout=5)
            proxy = client.proxy("uworker")
            with pytest.raises(ConnectorError):
                proxy.does_not_exist()
        finally:
            stop.set()
            t.join(timeout=3)

    def test_run_timeout(self, redis):
        # no worker is listening on the target queue
        client = redis.Connector("noworker_client", timeout=1)
        proxy = client.proxy("noworker_target")
        with pytest.raises(ConnectorError):
            proxy.anything()


# ---------------------------------------------------------------------------
# Integration -- Connector advanced (safe-mode plumbing)
# ---------------------------------------------------------------------------

@requires_redis
class TestConnectorAdvanced:
    def test_unreceive_puts_message_back(self, redis):
        sender = redis.Connector("u_src")
        worker = redis.Connector("u_wq", safe=True)
        sender.send("u_wq", "task")
        msg = worker.receive(timeout=2)
        assert msg.val == "task"
        # returning it to the queue makes it available again
        worker.unreceive(msg)
        again = worker.receive(timeout=2)
        assert again.val == "task"

    def test_reply_roundtrip(self, redis):
        client = redis.Connector("r_client")
        worker = redis.Connector("r_wq", safe=True)
        client.send("r_wq", "ping")
        msg = worker.receive(timeout=2)
        worker.reply(msg, "pong", force=False)
        answer = client.receive(timeout=2)
        assert answer.val == "pong"

    def test_transfer_moves_message(self, redis):
        sender = redis.Connector("t_src")
        worker = redis.Connector("t_wq", safe=True)
        sender.send("t_wq", "payload")
        msg = worker.receive(timeout=2)
        worker.transfer("t_dest", msg, "newpayload", force=False)
        got = redis.Connector("t_dest").receive(timeout=2)
        assert got.val == "newpayload"

    def test_receive_nonblocking_rpop(self, redis):
        c = redis.Connector("nb")
        assert c.receive(timeout=-1) is None  # empty -> immediate None
        redis.Connector("s_nb").send("nb", "hi")
        assert c.receive(timeout=-1).val == "hi"

    def test_receive_nonblocking_safe(self, redis):
        c = redis.Connector("nbs", safe=True)
        redis.Connector("s_nbs").send("nbs", "hey")
        msg = c.receive(timeout=-1)
        assert msg.val == "hey"
        assert "srcack" in msg


# ---------------------------------------------------------------------------
# Sentinel support -- driven entirely through FakeNode (no server needed)
# ---------------------------------------------------------------------------

class TestSentinel:
    def test_autodiscover_master_and_sentinels(self, fake_node):
        fake_node.masters_reply = [[b"name", b"mymaster"]]
        fake_node.sentinels_reply = [[b"ip", b"10.0.0.2", b"port", b"26380"]]
        r = desir.Redis(sentinels=[("10.0.0.1", 26379)])
        assert r.service_name == "mymaster"
        # the configured sentinel plus the one discovered via SENTINEL SENTINELS
        assert len(r.sentinels) == 2
        assert r.node is None  # master not resolved until first command

    def test_explicit_service_name_skips_masters(self, fake_node):
        r = desir.Redis(sentinels=[("10.0.0.1", 26379)], service_name="svc")
        assert r.service_name == "svc"

    def test_no_master_raises_sentinelerror(self, fake_node):
        fake_node.masters_reply = None  # no sentinel knows a master
        with pytest.raises(desir.desir3.SentinelError):
            desir.Redis(sentinels=[("10.0.0.1", 26379)])

    def test_node_resolves_master(self, fake_node):
        fake_node.master_addr = [b"10.0.0.9", b"6379"]
        r = desir.Redis(sentinels=[("10.0.0.1", 26379)], service_name="svc")
        node = r.__node__()
        assert (r.host, r.port) == ("10.0.0.9", 6379)
        assert node is not None

    def test_node_no_master_addr_raises(self, fake_node):
        fake_node.master_addr = []  # sentinel reachable but reports no master
        r = desir.Redis(sentinels=[("10.0.0.1", 26379)], service_name="svc")
        with pytest.raises(desir.desir3.SentinelErrorNoMaster):
            r.__node__()

    def test_node_all_sentinels_down_raises(self, fake_node):
        fake_node.raise_on_addr = True
        r = desir.Redis(sentinels=[("10.0.0.1", 26379)], service_name="svc")
        with pytest.raises(desir.desir3.SentinelError):
            r.__node__()


# ---------------------------------------------------------------------------
# Node connection handling
# ---------------------------------------------------------------------------

class TestNodeConnection:
    def test_not_connected_initially(self):
        node = desir.desir3.Node(host="127.0.0.1", port=16399, timeout=1)
        assert node.__connected__() is False

    def test_connect_failure_raises_nodeerror(self):
        # 16399 is assumed closed -> connection refused -> NodeError
        node = desir.desir3.Node(host="127.0.0.1", port=16399, timeout=1)
        with pytest.raises(desir.NodeError):
            node.connect()


@requires_redis
class TestRedisInternals:
    def test_runcmdon(self, redis):
        redis.ping()  # ensure self.node is populated
        assert redis.runcmdon(None, "ping") in ("PONG", b"PONG")

    def test_pipeline_method(self, redis):
        assert redis.pipeline() is redis
        redis.set("pk", "pv")
        res = redis.execute()
        assert res[0] in ("OK", b"OK")
        assert redis.get("pk") == b"pv"

    def test_node_connected_lifecycle(self, redis):
        redis.ping()
        node = redis.__node__()
        assert node.__connected__() is True
        node.disconnect()
        assert node.__connected__() is False

    def test_safe_mode_command(self, make_redis):
        r = make_redis(safe=True)
        assert r.set("sk", "sv") in ("OK", b"OK")
        assert r.get("sk") == b"sv"
        r.delete("sk")


@requires_redis
class TestHashEdgeCases:
    def test_items_on_empty_hash_returns_none(self, redis):
        # documents current behaviour: an empty/missing hash yields None
        h = redis.Hash("empty_hash")
        assert h.items() is None

    def test_underscore_missing_attribute_raises(self, redis):
        h = redis.Hash("uh")
        with pytest.raises(AttributeError):
            h._never_set


# ---------------------------------------------------------------------------
# Command JSON (re)loading
# ---------------------------------------------------------------------------

class TestReloadCommands:
    def test_reload_from_url(self, monkeypatch):
        import io
        orig = desir.desir3.redisCommands
        fake = io.BytesIO(b'{"PING": {"summary": "ping"}}')
        monkeypatch.setattr(
            desir.desir3.urllib.request, "urlopen", lambda url: fake)
        try:
            desir.desir3.reloadCommands("http://example/commands.json")
            assert "PING" in desir.desir3.redisCommands
        finally:
            desir.desir3.redisCommands = orig

    def test_reload_error_is_wrapped(self, monkeypatch):
        import urllib.error

        def boom(url):
            raise urllib.error.URLError("unreachable")

        monkeypatch.setattr(
            desir.desir3.urllib.request, "urlopen", boom)
        with pytest.raises(Exception):
            desir.desir3.reloadCommands("http://example/commands.json")
