import time
from redis.asyncio import Redis

# Redis Lua Script for Atomic Token Bucket Rate Limiting
# KEYS[1]: ratelimit:provider:{name}
# ARGV[1]: max_capacity (e.g., 80)
# ARGV[2]: refill_rate_per_sec (e.g., 80)
# ARGV[3]: current_timestamp (seconds as float)
TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local data = redis.call("HMGET", key, "tokens", "last_updated")
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])

if not tokens then
    tokens = capacity
    last_updated = now
else
    local elapsed = math.max(0, now - last_updated)
    tokens = math.min(capacity, tokens + elapsed * refill_rate)
    last_updated = now
end

if tokens >= 1 then
    tokens = tokens - 1
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    redis.call("EXPIRE", key, 3600)
    return {1, tokens} -- Allowed
else
    redis.call("HMSET", key, "tokens", tokens, "last_updated", last_updated)
    redis.call("EXPIRE", key, 3600)
    local wait_time = (1 - tokens) / refill_rate
    return {0, wait_time} -- Rate limited, return wait time in seconds
end
"""


class RedisTokenBucketRateLimiter:
    """Atomic Token-Bucket Rate Limiter per Provider backed by Redis Lua."""

    def __init__(self, redis: Redis):
        self.redis = redis
        self._lua_sha = None

    async def _ensure_script_loaded(self) -> str:
        if not self._lua_sha:
            self._lua_sha = await self.redis.script_load(TOKEN_BUCKET_LUA)
        return self._lua_sha

    async def acquire(self, provider_name: str, rate_limit_rps: int) -> tuple[bool, float]:
        """
        Attempts to acquire 1 token for a provider.
        Returns:
            (True, remaining_tokens) -> Token granted, fire request immediately.
            (False, wait_seconds)   -> Quota exceeded, wait seconds before retrying.
        """
        sha = await self._ensure_script_loaded()
        key = f"ratelimit:provider:{provider_name.upper()}"
        now = time.time()

        try:
            result = await self.redis.evalsha(sha, 1, key, rate_limit_rps, rate_limit_rps, now)
        except Exception:
            # Fallback to eval if script cache was cleared
            result = await self.redis.eval(TOKEN_BUCKET_LUA, 1, key, rate_limit_rps, rate_limit_rps, now)

        allowed = bool(result[0] == 1)
        value = float(result[1])
        return allowed, value
