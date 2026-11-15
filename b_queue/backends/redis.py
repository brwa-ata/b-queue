"""Jobs in Redis (pip install "b-queue[redis]").

    TASKS = {
        'default': {
            'BACKEND': 'b_queue.backends.RedisBackend',
            'OPTIONS': {'URL': 'redis://localhost:6379/0'},
        }
    }

Keys (all under one hash tag, so they share a Redis Cluster slot):

    {prefix}:job:<id>        hash: data (the JobRecord as JSON) + the few
                             fields the Lua scripts read
    {prefix}:queue:<name>    zset of ready job ids; score orders by priority,
                             then time
    {prefix}:delayed         zset of ids not available yet; score = when
    {prefix}:reserved        zset of running ids; score = when they are lost
    {prefix}:failed          zset of failed ids; score = when they failed
    {prefix}:queues          set of every queue name seen
    {prefix}:unique:<key>    id of the job holding a unique key
    {prefix}:lock:<key>      without_overlapping locks
    {prefix}:schedule        hash: schedule entry -> last slot queued

Every step that moves a job between those structures is one Lua script, so a
job is never lost or run twice by a crash or by two workers racing.

Redis has no transaction to share with your database, so by default an
enqueue made inside `transaction.atomic()` is sent when it commits
(ENQUEUE_ON_COMMIT).
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta

from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_finished
from django.utils import timezone
from django.utils.functional import cached_property

from b_queue.backends.base import QueueBackend
from b_queue.record import FAILED, READY, RUNNING, SUCCESSFUL, JobRecord, build_result

# score of a ready job: higher priority first, then first in first out
PRIORITY_STEP = 10**13

# KEYS: job, target zset, queues set, unique key
# ARGV: id, data, queue, priority, timeout, score, has_unique, unique_until,
#       unique_ttl, job key prefix
ENQUEUE = """
if ARGV[7] == '1' then
    local holder = redis.call('GET', KEYS[4])
    if holder then
        local status = redis.call('HGET', ARGV[10] .. holder, 'status')
        local blocking = status == 'READY'
        if status == 'RUNNING' then
            blocking = redis.call('HGET', ARGV[10] .. holder, 'unique_until') ~= 'start'
        end
        if blocking then
            return holder
        end
    end
    if tonumber(ARGV[9]) > 0 then
        redis.call('SET', KEYS[4], ARGV[1], 'EX', ARGV[9])
    else
        redis.call('SET', KEYS[4], ARGV[1])
    end
end
redis.call('HSET', KEYS[1], 'data', ARGV[2], 'status', 'READY', 'queue', ARGV[3],
           'priority', ARGV[4], 'timeout', ARGV[5], 'unique_until', ARGV[8])
redis.call('ZADD', KEYS[2], ARGV[6], ARGV[1])
redis.call('SADD', KEYS[3], ARGV[3])
return false
"""

# Move due delayed jobs to their queues, then pop the next job.
# KEYS: delayed, reserved, then one key per queue to look in
# ARGV: now (seconds), token, lost grace, job key prefix, queue key prefix
RESERVE = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 200)
for _, id in ipairs(due) do
    local job = ARGV[4] .. id
    local queue = redis.call('HGET', job, 'queue')
    if queue then
        local priority = tonumber(redis.call('HGET', job, 'priority')) or 0
        local at = redis.call('ZSCORE', KEYS[1], id) * 1000
        redis.call('ZADD', ARGV[5] .. queue, at - priority * 10000000000000, id)
    end
    redis.call('ZREM', KEYS[1], id)
end

for i = 3, #KEYS do
    while true do
        local popped = redis.call('ZPOPMIN', KEYS[i], 1)
        if #popped == 0 then
            break
        end
        local id = popped[1]
        local job = ARGV[4] .. id
        if redis.call('EXISTS', job) == 1 then
            local timeout = tonumber(redis.call('HGET', job, 'timeout')) or 600
            redis.call('HSET', job, 'token', ARGV[2], 'status', 'RUNNING')
            redis.call('ZADD', KEYS[2], ARGV[1] + timeout + ARGV[3], id)
            return id
        end
    end
end
return false
"""

# Write the outcome of a reserved job, if the reservation still stands.
# KEYS: job, reserved, target zset ('-' key when none)
# ARGV: token, id, data, status, has_target, score, expire seconds
SAVE = """
if redis.call('HGET', KEYS[1], 'token') ~= ARGV[1] then
    return 0
end
redis.call('HSET', KEYS[1], 'data', ARGV[3], 'status', ARGV[4])
redis.call('HDEL', KEYS[1], 'token')
redis.call('ZREM', KEYS[2], ARGV[2])
if ARGV[5] == '1' then
    redis.call('ZADD', KEYS[3], ARGV[6], ARGV[2])
end
if tonumber(ARGV[7]) > 0 then
    redis.call('EXPIRE', KEYS[1], ARGV[7])
end
return 1
"""

# KEYS: key ; ARGV: expected value
DELETE_IF_EQUAL = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""

# KEYS: schedule hash ; ARGV: entry name, slot (seconds)
SCHEDULE_CLAIM = """
local last = redis.call('HGET', KEYS[1], ARGV[1])
if last and tonumber(last) >= tonumber(ARGV[2]) then
    return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
return 1
"""


class RedisBackend(QueueBackend):
    enqueue_on_commit_default = True

    def __init__(self, alias, params):
        super().__init__(alias, params)
        self.url = self.options.get('URL', 'redis://localhost:6379/0')
        self.prefix = '{' + self.options.get('PREFIX', 'b_queue') + '}:'

    @cached_property
    def client(self):
        try:
            import redis
        except ImportError as error:
            raise ImportError(
                'The Redis backend needs the redis package: pip install "b-queue[redis]"'
            ) from error
        return redis.Redis.from_url(self.url, decode_responses=True)

    def _key(self, *parts) -> str:
        return self.prefix + ':'.join(parts)

    def _script(self, source):
        # register_script caches by SHA and falls back to EVAL when needed
        scripts = self.__dict__.setdefault('_scripts', {})
        if source not in scripts:
            scripts[source] = self.client.register_script(source)
        return scripts[source]

    def _load(self, job_id) -> JobRecord | None:
        data, token = self.client.hmget(self._key('job', job_id), 'data', 'token')
        if data is None:
            return None
        record = JobRecord.from_json(data)
        record.token = token
        return record

    @staticmethod
    def _ready_score(record: JobRecord, at: datetime) -> float:
        return at.timestamp() * 1000 - record.priority * PRIORITY_STEP

    def _target(self, record: JobRecord, now: datetime):
        """Where a ready job waits: its queue, or the delayed set."""
        if record.available_at > now:
            return self._key('delayed'), record.available_at.timestamp()
        return self._key('queue', record.queue_name), self._ready_score(record, now)

    # -- enqueue / results ------------------------------------------------
    def store(self, record):
        target, score = self._target(record, timezone.now())
        holder = self._script(ENQUEUE)(
            keys=[
                self._key('job', record.id),
                target,
                self._key('queues'),
                self._key('unique', record.unique_key or '-'),
            ],
            args=[
                record.id,
                record.to_json(),
                record.queue_name,
                record.priority,
                record.timeout,
                score,
                '1' if record.unique_key else '0',
                record.unique_until,
                record.unique_for or 0,
                self._key('job', ''),
            ],
        )
        if holder:
            return self._load(holder) or self.store(record)
        return None

    def get_record(self, job_id):
        record = self._load(job_id)
        if record is None:
            raise TaskResultDoesNotExist(job_id)
        return record

    # -- worker -----------------------------------------------------------
    def reserve(self, queues, worker_id, now=None):
        if '*' in queues:
            queues = sorted(self.client.smembers(self._key('queues')))

        started = timezone.now()
        look_until = now or started
        token = secrets.token_hex(8)
        job_id = self._script(RESERVE)(
            keys=[
                self._key('delayed'),
                self._key('reserved'),
                *[self._key('queue', queue) for queue in queues],
            ],
            args=[
                look_until.timestamp(),
                token,
                self.lost_grace,
                self._key('job', ''),
                self._key('queue', ''),
            ],
        )
        if not job_id:
            return None

        record = self._load(job_id)
        record.status = RUNNING
        record.started_at = record.last_attempted_at = started
        record.timeout_at = started + timedelta(
            seconds=record.timeout + self.lost_grace
        )
        record.worker_ids.append(worker_id)

        pipe = self.client.pipeline()
        pipe.hset(self._key('job', record.id), 'data', record.to_json())
        # the script scored the job by `look_until`; score it by the clock
        pipe.zadd(
            self._key('reserved'), {record.id: record.timeout_at.timestamp()}, xx=True
        )
        pipe.execute()

        if record.unique_key and record.unique_until == 'start':
            self._release_unique(record.unique_key, record.id)
        return record

    def save(self, record):
        now = timezone.now()
        finished = record.status in (FAILED, SUCCESSFUL)

        if record.status == READY:
            target, score = self._target(record, now)
        elif record.status == FAILED:
            target, score = self._key('failed'), now.timestamp()
        else:
            target, score = None, 0

        expire = 0
        if record.status == SUCCESSFUL and self.keep_results is not None:
            expire = max(int(self.keep_results), 1)

        unique_key = record.unique_key
        if finished:
            record.unique_key = None

        saved = self._script(SAVE)(
            keys=[
                self._key('job', record.id),
                self._key('reserved'),
                target or self._key('-'),
            ],
            args=[
                record.token or '',
                record.id,
                record.to_json(),
                record.status,
                '1' if target else '0',
                score,
                expire,
            ],
        )
        if saved and finished and unique_key:
            self._release_unique(unique_key, record.id)
        return bool(saved)

    def _release_unique(self, unique_key, job_id):
        self._script(DELETE_IF_EQUAL)(
            keys=[self._key('unique', unique_key)], args=[job_id]
        )

    def recover_lost(self, now=None):
        now = now or timezone.now()
        reserved = self._key('reserved')
        recovered = 0

        for job_id in self.client.zrangebyscore(reserved, '-inf', now.timestamp()):
            record = self._load(job_id)
            if record is None:
                self.client.zrem(reserved, job_id)
                continue

            if record.status != RUNNING:
                # the worker died right after reserving: the run never began
                record.worker_ids.append('lost')
            self.lost_outcome(record, now)
            if not self.save(record):
                continue
            recovered += 1
            if record.status == FAILED:
                try:
                    result = build_result(record)
                except Exception:
                    continue  # the task's code is gone
                task_finished.send(sender=type(self), task_result=result)

        return recovered

    # -- locks ------------------------------------------------------------
    def acquire_lock(self, key, owner, ttl):
        return bool(
            self.client.set(self._key('lock', key), owner, nx=True, ex=max(int(ttl), 1))
        )

    def release_lock(self, key, owner):
        self._script(DELETE_IF_EQUAL)(keys=[self._key('lock', key)], args=[owner])

    # -- scheduler state --------------------------------------------------
    def schedule_claim(self, name, due, now):
        return bool(
            self._script(SCHEDULE_CLAIM)(
                keys=[self._key('schedule')], args=[name, int(due.timestamp())]
            )
        )

    def schedule_forget(self, keep):
        gone = set(self.client.hkeys(self._key('schedule'))) - set(keep)
        if gone:
            self.client.hdel(self._key('schedule'), *gone)

    # -- management -------------------------------------------------------
    def failed_jobs(self):
        records = [
            self._load(i) for i in self.client.zrange(self._key('failed'), 0, -1)
        ]
        return [record for record in records if record is not None]

    def retry_job(self, job_id):
        if not self.client.zrem(self._key('failed'), job_id):
            return False
        record = self._load(job_id)
        if record is None:
            return False

        now = timezone.now()
        record.status = READY
        record.available_at = now
        record.finished_at = record.timeout_at = None
        record.worker_ids = []

        pipe = self.client.pipeline()
        pipe.hset(
            self._key('job', job_id),
            mapping={'data': record.to_json(), 'status': READY},
        )
        pipe.zadd(
            self._key('queue', record.queue_name),
            {job_id: self._ready_score(record, now)},
        )
        pipe.execute()
        return True

    def forget_job(self, job_id):
        if not self.client.zrem(self._key('failed'), job_id):
            return False
        self.client.delete(self._key('job', job_id))
        return True

    def flush_failed(self):
        return self._delete_members(self._key('failed'), '-inf', '+inf')

    def prune(self, before, include_failed=False):
        # successful jobs expire by themselves (KEEP_RESULTS)
        if not include_failed:
            return 0
        return self._delete_members(self._key('failed'), '-inf', before.timestamp())

    def clear(self, queue=None):
        if queue:
            return self._delete_members(self._key('queue', queue), '-inf', '+inf')

        count = self._delete_members(self._key('delayed'), '-inf', '+inf')
        for name in self.client.smembers(self._key('queues')):
            count += self._delete_members(self._key('queue', name), '-inf', '+inf')
        return count

    def _delete_members(self, zset, low, high) -> int:
        ids = self.client.zrangebyscore(zset, low, high)
        if ids:
            pipe = self.client.pipeline()
            pipe.delete(*[self._key('job', job_id) for job_id in ids])
            pipe.zrem(zset, *ids)
            pipe.execute()
        return len(ids)

    def stats(self):
        names = sorted(self.client.smembers(self._key('queues')))
        pipe = self.client.pipeline()
        for name in names:
            pipe.zcard(self._key('queue', name))
        pipe.zcard(self._key('delayed'))
        pipe.zcard(self._key('reserved'))
        pipe.zcard(self._key('failed'))
        *sizes, delayed, running, failed = pipe.execute()

        queues = {name: size for name, size in zip(names, sizes) if size}
        return {
            'ready': sum(queues.values()),
            'delayed': delayed,
            'running': running,
            'failed': failed,
            'queues': queues,
        }
