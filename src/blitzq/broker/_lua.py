"""Lua scripts used by the Redis brokers.

Scripts make multi-key transitions atomic. ``PROMOTE`` derives queue keys
from the namespace prefix at run time (the target queue is only known once a
scheduled entry is read), so BlitzQ does not support Redis Cluster; see
docs/operations.md.
"""

# Push helper shared by several scripts: ARGV mode 'l' = list (fast), 's' = stream (reliable).
_PUSH = """
local function bq_push(mode, key, data)
  if mode == 'l' then
    redis.call('LPUSH', key, data)
  else
    redis.call('XADD', key, '*', 'd', data)
  end
end
"""

# A scheduled entry packs its queue name and message into one hash field
# ("queue\0message") instead of two separate hashes, halving the Redis
# commands spent on every delayed task, retry and periodic dispatch.
_UNPACK = """
local function bq_unpack(v)
  local i = string.find(v, "\\0", 1, true)
  return string.sub(v, 1, i - 1), string.sub(v, i + 1)
end
"""

# KEYS: sched zset, sched data hash
# ARGV: now, limit, mode, key prefix for queues (e.g. "blitzq:l:")
# Returns {promoted, next_eta_or_false}
PROMOTE = (
    _PUSH
    + _UNPACK
    + """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local n = 0
for _, id in ipairs(ids) do
  local packed = redis.call('HGET', KEYS[2], id)
  redis.call('ZREM', KEYS[1], id)
  redis.call('HDEL', KEYS[2], id)
  if packed then
    local q, data = bq_unpack(packed)
    bq_push(ARGV[3], ARGV[4] .. q, data)
    n = n + 1
  end
end
local nxt = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if nxt[2] then
  return {n, nxt[2]}
end
return {n, false}
"""
)

# KEYS: sched zset, sched data hash, revoked zset
# ARGV: task id, revocation expiry (epoch seconds), now
CANCEL = """
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', ARGV[3])
if redis.call('ZREM', KEYS[1], ARGV[1]) == 1 then
  redis.call('HDEL', KEYS[2], ARGV[1])
  return 1
end
redis.call('ZADD', KEYS[3], ARGV[2], ARGV[1])
return 0
"""

# KEYS: dlq hash, dlq index zset
# ARGV: task id, record, failed_at, max_entries (0 = unbounded)
DLQ_ADD = """
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
local maxn = tonumber(ARGV[4])
if maxn > 0 then
  local excess = redis.call('ZCARD', KEYS[2]) - maxn
  if excess > 0 then
    local old = redis.call('ZRANGE', KEYS[2], 0, excess - 1)
    for _, id in ipairs(old) do
      redis.call('HDEL', KEYS[1], id)
    end
    redis.call('ZREMRANGEBYRANK', KEYS[2], 0, excess - 1)
  end
end
return 1
"""

# KEYS: dlq hash, dlq index zset, target queue key, task record key
# ARGV: task id, new message, mode
DLQ_REPLAY = (
    _PUSH
    + """
if redis.call('HDEL', KEYS[1], ARGV[1]) == 0 then
  return 0
end
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('DEL', KEYS[4])
bq_push(ARGV[3], KEYS[3], ARGV[2])
return 1
"""
)

# KEYS: periodic hash, target queue key
# ARGV: name, occurrence, mode, has_data ('1'/'0'), data
PERIODIC_CLAIM = (
    _PUSH
    + """
local last = redis.call('HGET', KEYS[1], ARGV[1])
if last and tonumber(last) >= tonumber(ARGV[2]) then
  return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
if ARGV[4] == '1' then
  bq_push(ARGV[3], KEYS[2], ARGV[5])
end
return 1
"""
)

# Reliable mode lease renewal. Resets the idle time of entries still owned by
# this consumer; entries recovered by another consumer are reported back.
# KEYS: stream
# ARGV: group, consumer, id...
HEARTBEAT = """
local lost = {}
for i = 3, #ARGV do
  local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[i], ARGV[i], 1)
  if p[1] and p[1][2] == ARGV[2] then
    redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, ARGV[i], 'JUSTID')
  else
    table.insert(lost, ARGV[i])
  end
end
return lost
"""

# Token bucket, shared across every caller of this key. Lazily initialised
# (a bucket that has never been touched starts full) and self-expiring (no
# bucket lives longer than one refill cycle past its last use, so an unused
# rate limit leaves nothing behind in Redis).
# KEYS: bucket hash
# ARGV: now, rate (tokens/sec), capacity
# Returns {allowed (0/1), wait_seconds}
RATE_LIMIT = """
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts = now
end
tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate)
local allowed = 0
local wait = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  wait = (1 - tokens) / rate
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', KEYS[1], math.ceil(capacity / rate) + 1)
return {allowed, tostring(wait)}
"""

# Idempotency. One hash per key ({task, state, result}) plus a separate
# execution lock with a lease, so a crashed owner's lock expires by itself.
#
# Enqueue-time claim. Returns the task id already holding the key, or nil if
# this call took it (and may publish).
# KEYS: hash   ARGV: task_id, ttl_seconds
IDEM_CLAIM = """
local cur = redis.call('HGET', KEYS[1], 'task')
if cur then return cur end
redis.call('HSET', KEYS[1], 'task', ARGV[1], 'state', 'queued')
redis.call('EXPIRE', KEYS[1], ARGV[2])
return false
"""

# Give the key back (publish failed, or the job ended without succeeding) so a
# later enqueue with the same key can run. A completed key is never released.
# KEYS: hash   ARGV: task_id
IDEM_UNCLAIM = """
if redis.call('HGET', KEYS[1], 'state') == 'done' then return 0 end
if redis.call('HGET', KEYS[1], 'task') ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
return 1
"""

# Execution gate, run just before the task body.
# Returns {'done', result} if the effect already happened, {'busy', pttl_ms}
# if another live execution holds the key, or {'run', 0} after taking the lock.
# KEYS: hash, lock   ARGV: owner, lease_ms
IDEM_BEGIN = """
if redis.call('HGET', KEYS[1], 'state') == 'done' then
  return {'done', redis.call('HGET', KEYS[1], 'result') or ''}
end
local holder = redis.call('GET', KEYS[2])
if holder and holder ~= ARGV[1] then
  return {'busy', redis.call('PTTL', KEYS[2])}
end
redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[2])
return {'run', 0}
"""

# Extend the lock while the task is still running; 0 means it was lost.
# KEYS: lock   ARGV: owner, lease_ms
IDEM_RENEW = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
"""

# Record the outcome and drop our lock. A success is recorded even if the lock
# was lost meanwhile (the effect happened), but the first recorded result wins
# so every later duplicate sees one consistent answer.
# KEYS: hash, lock   ARGV: owner, success (0/1), task_id, result, ttl_seconds
IDEM_FINISH = """
if ARGV[2] == '1' and redis.call('HGET', KEYS[1], 'state') ~= 'done' then
  redis.call('HSET', KEYS[1], 'task', ARGV[3], 'state', 'done', 'result', ARGV[4])
  redis.call('EXPIRE', KEYS[1], ARGV[5])
end
if redis.call('GET', KEYS[2]) == ARGV[1] then
  redis.call('DEL', KEYS[2])
end
return 1
"""
