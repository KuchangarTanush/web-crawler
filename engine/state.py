"""
Crawl state, per the design doc's "Crawl State" section: pending work,
completed work, retries, source health, and incremental positions.

Backed by Redis so it's shared correctly across multiple Celery workers,
not a local file that would race under concurrent access.

One hash per source at key crawlstate:<source_name> with fields:
    last_crawled_ts       - unix timestamp of last completed attempt (any outcome)
    last_success_ts       - unix timestamp of last successful fetch
    last_status           - "success" | "failed" | "dead_letter" | "in_progress"
    consecutive_failures  - resets to 0 on success; used for source health + backoff
    total_success / total_failed - running counters
    in_progress           - "1" while a job for this source is executing
    started_ts            - when the current job started (lets stale flags expire)
    etag / last_modified  - HTTP validators for conditional GET (incremental crawling)
    last_content_hash     - sha256 of the last stored content
"""

import os
import time

import redis


REDIS_URL = os.environ.get(
    "CRAWL_STATE_REDIS_URL",
    "redis://localhost:6379/2"
)

# A source that keeps failing is retried less and less often:
#   interval x 2, x 4, x 8 (capped) after 1, 2, 3+ consecutive failures.
MAX_BACKOFF_FACTOR = 8


class CrawlState:
    def __init__(self, redis_url: str = REDIS_URL):
        self.r = redis.Redis.from_url(
            redis_url,
            decode_responses=True
        )

    def _key(self, source_name: str) -> str:
        return f"crawlstate:{source_name}"

    # ------------------------------------------------------------------ #
    # Bulk read (one Redis round trip for ALL sources)
    # ------------------------------------------------------------------ #
    def snapshot(self, source_names: list[str]) -> dict:
        """
        Fetch the state hash of every source in ONE round trip.
        The scheduler used to do 2-3 sequential Redis calls per source on
        every 30-second tick; this does one pipelined call in total.
        """
        if not source_names:
            return {}
        pipe = self.r.pipeline(transaction=False)
        for name in source_names:
            pipe.hgetall(self._key(name))
        return dict(zip(source_names, pipe.execute()))

    @staticmethod
    def in_progress_from(state: dict, stale_after: float) -> bool:
        """
        True only if a job is genuinely running. A flag older than
        `stale_after` seconds is ignored, so a worker that was killed
        mid-job can't block its source forever.
        """
        if state.get("in_progress") != "1":
            return False
        started = state.get("started_ts")
        if started is None:
            return True
        return (time.time() - float(started)) < stale_after

    @staticmethod
    def due_from(state: dict, interval_minutes: int) -> bool:
        last = state.get("last_crawled_ts")
        if last is None:
            return True

        failures = int(state.get("consecutive_failures") or 0)
        factor = min(2 ** failures, MAX_BACKOFF_FACTOR)

        return (time.time() - float(last)) >= interval_minutes * 60 * factor

    # ------------------------------------------------------------------ #
    # Single-source checks (kept for scripts / backwards compatibility)
    # ------------------------------------------------------------------ #
    def is_in_progress(self, source_name: str, stale_after: float = 7200) -> bool:
        return self.in_progress_from(self.get_health(source_name), stale_after)

    def is_due(self, source_name: str, interval_minutes: int) -> bool:
        return self.due_from(self.get_health(source_name), interval_minutes)

    # ------------------------------------------------------------------ #
    # Locking
    # ------------------------------------------------------------------ #
    def acquire_lock(self, source_name: str, ttl: int = 3600) -> bool:
        """
        Atomically claim a source so the scheduler cannot enqueue
        duplicate crawl jobs. The TTL is only a safety net for a job
        that dies without releasing the lock.
        """
        return bool(
            self.r.set(
                f"crawl-lock:{source_name}",
                "1",
                nx=True,
                ex=ttl,
            )
        )

    def release_lock(self, source_name: str) -> None:
        """Release the scheduler/worker lock for a source."""
        self.r.delete(f"crawl-lock:{source_name}")

    # ------------------------------------------------------------------ #
    # Job lifecycle
    # ------------------------------------------------------------------ #
    def mark_started(self, source_name: str) -> None:
        self.r.hset(
            self._key(source_name),
            mapping={
                "in_progress": "1",
                "started_ts": time.time(),
                "last_status": "in_progress",
            }
        )

    def mark_success(self, source_name: str, record: dict | None = None) -> None:
        """
        Record a successful crawl. If the crawl-result `record` is passed,
        its HTTP validators and content hash are stored so the NEXT crawl
        can send a conditional request (If-None-Match / If-Modified-Since).
        """
        now = time.time()
        key = self._key(source_name)

        pipe = self.r.pipeline()
        pipe.hset(
            key,
            mapping={
                "last_crawled_ts": now,
                "last_success_ts": now,
                "last_status": "success",
                "consecutive_failures": 0,
            }
        )
        pipe.hdel(key, "last_error")
        pipe.hincrby(key, "total_success", 1)

        if record:
            validators = {
                "etag": record.get("etag"),
                "last_modified": record.get("last_modified"),
                "last_content_hash": record.get("content_hash"),
            }
            for field, value in validators.items():
                if value:
                    pipe.hset(key, field, value)
                else:
                    pipe.hdel(key, field)

        pipe.execute()

    def get_validators(self, source_name: str) -> dict:
        """Validators for a conditional GET; empty dict if we have none."""
        h = self.r.hmget(
            self._key(source_name),
            "etag", "last_modified", "last_content_hash"
        )
        etag, last_modified, content_hash = h
        # A 304 is only useful if we also know which content it refers to.
        if not content_hash or not (etag or last_modified):
            return {}
        return {
            "etag": etag,
            "last_modified": last_modified,
            "prev_hash": content_hash,
        }

    def mark_retry(
        self,
        source_name: str,
        reason: str
    ) -> None:
        self.r.hset(
            self._key(source_name),
            mapping={
                "last_status": "retrying",
                "last_error": reason[:500],
            }
        )

    def mark_dead_letter(
        self,
        source_name: str,
        reason: str
    ) -> None:
        now = time.time()
        key = self._key(source_name)

        pipe = self.r.pipeline()
        pipe.hset(
            key,
            mapping={
                "last_crawled_ts": now,
                "last_status": "dead_letter",
                "last_error": reason[:500],
            }
        )
        pipe.hincrby(key, "total_failed", 1)
        pipe.hincrby(key, "consecutive_failures", 1)
        pipe.execute()

    def mark_finished(self, source_name: str) -> None:
        self.r.hset(
            self._key(source_name),
            "in_progress",
            "0"
        )

    def get_health(self, source_name: str) -> dict:
        return self.r.hgetall(
            self._key(source_name)
        )

    def all_states(
        self,
        source_names: list[str]
    ) -> dict:
        return self.snapshot(source_names)
