from sys import version_info
if version_info[0] == 3:
    from .desir3 import SubAsync,Node,Redis,RedisError,NodeError,ConnectorError,SWM,RedisInner
else:
    from .desir import SubAsync,Node,Redis,RedisError,NodeError,ConnectorError,SWM,RedisInner
