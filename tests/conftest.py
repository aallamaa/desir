"""Shared pytest fixtures for the desir test-suite.

The integration tests need a reachable Redis server. By default they talk to
localhost:6379 on database 9 (assumed disposable). Override with the
DESIR_TEST_HOST / DESIR_TEST_PORT / DESIR_TEST_DB environment variables.

If no Redis server can be reached, every test depending on the ``redis``
fixture is skipped rather than failing, so the pure-logic tests still run.
"""
import os

import pytest

import desir


TEST_HOST = os.environ.get("DESIR_TEST_HOST", "localhost")
TEST_PORT = int(os.environ.get("DESIR_TEST_PORT", "6379"))
TEST_DB = int(os.environ.get("DESIR_TEST_DB", "9"))


def _redis_available():
    try:
        r = desir.Redis(host=TEST_HOST, port=TEST_PORT, db=TEST_DB, timeout=1)
        return r.ping() in ("PONG", b"PONG")
    except Exception:
        return False


REDIS_AVAILABLE = _redis_available()
requires_redis = pytest.mark.skipif(
    not REDIS_AVAILABLE,
    reason="no Redis server reachable at %s:%s" % (TEST_HOST, TEST_PORT),
)


@pytest.fixture
def redis():
    """A clean Redis client bound to the disposable test database.

    The database is flushed before and after each test so cases are isolated.
    """
    if not REDIS_AVAILABLE:
        pytest.skip("no Redis server reachable")
    r = desir.Redis(host=TEST_HOST, port=TEST_PORT, db=TEST_DB, timeout=5)
    r.flushdb()
    try:
        yield r
    finally:
        r.flushdb()


@pytest.fixture
def redis_params():
    """Raw connection kwargs, for APIs that build their own client (SubAsync)."""
    if not REDIS_AVAILABLE:
        pytest.skip("no Redis server reachable")
    return dict(host=TEST_HOST, port=TEST_PORT, db=TEST_DB, timeout=5)


@pytest.fixture
def make_redis():
    """Factory for additional independent clients (e.g. a second connection).

    Clients created here are NOT auto-flushed; use the ``redis`` fixture for
    the database lifecycle.
    """
    if not REDIS_AVAILABLE:
        pytest.skip("no Redis server reachable")

    def _factory(**kwargs):
        params = dict(host=TEST_HOST, port=TEST_PORT, db=TEST_DB, timeout=5)
        params.update(kwargs)
        return desir.Redis(**params)

    return _factory
