"""
Scheduler: the periodic tick that decides what to crawl next.

"The scheduler checks which sources are due for crawling and creates the
required tasks" (design doc). Runs as a Celery beat task on a fixed
interval (SCHEDULER_TICK_SECONDS, default 30s). For each enabled source:
  - skip if a job for it is already in progress
  - skip if it isn't due yet (failing sources back off automatically)
  - acquire a Redis lock to prevent duplicate jobs
  - enqueue a crawl job on the queue matching its priority

Efficiency: the state of ALL sources is read in a single pipelined Redis
call, and sources.yaml is only re-parsed when the file changes.
"""

import logging

from engine.celery_app import app
from engine.runner import lock_ttl_for
from engine.sources import load_sources
from engine.state import CrawlState
from engine.tasks import crawl_source


logger = logging.getLogger(__name__)

state = CrawlState()


# ignore_result: this runs every tick; there's no reason to write its return
# value to the Redis result backend 2,880 times a day.
@app.task(ignore_result=True)
def check_and_schedule():
    scheduled = []

    try:
        # strict=False: a bad entry is logged and skipped on its own,
        # it no longer stops every other source from being scheduled.
        sources = [
            s for s in load_sources(strict=False)
            if s.get("enabled", True)
        ]
    except Exception:
        logger.exception("Could not load sources.yaml; nothing scheduled this tick")
        return {"scheduled": scheduled}

    # One Redis round trip for every source's state.
    snapshot = state.snapshot([s["name"] for s in sources])

    for source in sources:

        name = source["name"]
        priority = source["priority"]
        interval_minutes = source["interval_minutes"]
        source_state = snapshot.get(name, {})
        ttl = lock_ttl_for(source)

        # Skip if this source is already being crawled
        # (a flag older than the lock lifetime is treated as stale)
        if state.in_progress_from(source_state, stale_after=ttl):
            logger.debug("Skipping '%s': already in progress", name)
            continue

        # Skip if the source is not due yet
        if not state.due_from(source_state, interval_minutes):
            continue

        # Atomically claim the source before putting the job
        # into the Celery queue.
        if not state.acquire_lock(name, ttl=ttl):
            logger.debug("Skipping '%s': already claimed", name)
            continue

        try:
            crawl_source.apply_async(
                args=[source],
                queue=priority,
            )
        except Exception:
            # Broker hiccup: don't leave the source locked (and therefore
            # un-crawlable) until the TTL expires.
            state.release_lock(name)
            logger.exception("Failed to enqueue '%s'; lock released", name)
            continue

        scheduled.append(name)

        logger.info(
            "Scheduled crawl for '%s' on queue '%s'",
            name,
            priority,
        )

    return {
        "scheduled": scheduled
    }
