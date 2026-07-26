"""Redis 热点缓存(设计方案 12 章):血缘日更,TTL 到次日重建时刻。

Redis 不可用时静默直查 MySQL——缓存是加速器不是依赖。
"""

import hashlib
import json
import logging
from datetime import datetime, timedelta

from . import config

log = logging.getLogger(__name__)

_client = None
_client_failed = False


def _redis():
    global _client, _client_failed
    if _client is None and not _client_failed and config.REDIS_URL:
        try:
            import redis
            _client = redis.from_url(config.REDIS_URL, socket_timeout=2,
                                     socket_connect_timeout=2)
            _client.ping()
        except Exception as e:
            log.warning("redis unavailable, cache disabled: %s", e)
            _client, _client_failed = None, True
    return _client


def _ttl_to_next_rebuild() -> int:
    now = datetime.now()
    nxt = now.replace(hour=config.REBUILD_HOUR, minute=0, second=0, microsecond=0)
    if nxt <= now:
        nxt += timedelta(days=1)
    return max(int((nxt - now).total_seconds()), 60)


def cached(prefix: str, args: tuple, compute):
    client = _redis()
    if client is None:
        return compute()
    key = f"dv:{prefix}:" + hashlib.sha256(repr(args).encode()).hexdigest()[:24]
    try:
        hit = client.get(key)
        if hit is not None:
            return json.loads(hit)
    except Exception:
        return compute()
    result = compute()
    try:
        if isinstance(result, dict) and not result.get("error"):
            client.setex(key, _ttl_to_next_rebuild(),
                         json.dumps(result, ensure_ascii=False, default=str))
    except Exception:
        pass
    return result
