from sys import version_info
if version_info[0] == 3:
    from .desir3 import (SubAsync, Node, Redis, RedisError,
                         NodeError, RedisInner, reloadCommands,
                         parse_version, COMMANDS_URL)
    from .sugar import ConnectorError, SWM, Connector, LockError, Stream
else:
    from .desir import SubAsync, Node, Redis, RedisError
    from .desir import NodeError, ConnectorError, SWM, RedisInner
