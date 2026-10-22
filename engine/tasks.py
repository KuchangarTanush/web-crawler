"""
Worker task: fetch one source.

The actual crawl runs as a clean Scrapy subprocess (see engine.runner).

This is a second, separate layer of retry from Scrapy's own per-request
BackoffRetryMiddleware: that one retries individual HTTP requests inside
a crawl. This one retries the whole crawl job if the process itself fails
(crash, timeout, non-zero exit, failed fetch).

Permanent failures (e.g. HTTP 404 / 403) skip the job retries and go
straight to the dead-letter log: repeating them would only waste worker time.
"""

import json
import logging
import os
import time

from engine.celery_app import app
from engine.runner import (
    JOB_RETRY_BASE_DELAY,
    MAX_JOB_RETRIES,
    PROJECT_ROOT,
    PermanentCrawlError,
    run_crawl,
)
from engine.state import CrawlState


logger = logging.getLogger(__name__)

state = CrawlState()

DEAD_LETTER_LOG = os.path.join(
    PROJECT_ROOT,
    "crawl_output",
    "dead_letter.jsonl"
)


@app.task(
    bind=True,
    max_retries=MAX_JOB_RETRIES,
    ignore_result=False
)
def crawl_source(self, source: dict):

    name = source["name"]

    # Mark the source as currently executing
    state.mark_started(name)

    try:
        # Validators from the last successful crawl (ETag / Last-Modified)
        # let the server answer "304 Not Modified" instead of resending the page.
        record = run_crawl(source, state.get_validators(name))

        # ---------------------------------------------------------
        # SUCCESS
        # ---------------------------------------------------------

        state.mark_success(name, record)
        state.mark_finished(name)
        state.release_lock(name)

        unchanged = bool(record.get("unchanged"))

        logger.info(
            "Crawl succeeded for source '%s'%s",
            name,
            " (unchanged, 304)" if unchanged else "",
        )

        return {
            "source": name,
            "status": "success",
            "unchanged": unchanged,
            "http_status": record.get("http_status"),
        }

    except PermanentCrawlError as exc:

        # ---------------------------------------------------------
        # PERMANENT FAILURE -> dead-letter now, no job retries
        # ---------------------------------------------------------

        logger.error(
            "Permanent failure for '%s' (not retrying): %s", name, exc
        )

        return _dead_letter(source, exc)

    except Exception as exc:

        attempt = self.request.retries

        # ---------------------------------------------------------
        # RETRY
        # ---------------------------------------------------------

        if attempt < MAX_JOB_RETRIES:

            delay = JOB_RETRY_BASE_DELAY * (2 ** attempt)

            state.mark_retry(
                name,
                str(exc)
            )

            logger.warning(
                "Crawl job failed for '%s' "
                "(attempt %d/%d), retrying in %ds: %s",
                name,
                attempt + 1,
                MAX_JOB_RETRIES,
                delay,
                exc,
            )

            # IMPORTANT:
            # Do NOT call mark_finished() here.
            # Do NOT release the lock here.
            #
            # The job is still active because Celery will retry it.
            raise self.retry(
                exc=exc,
                countdown=delay
            )

        # ---------------------------------------------------------
        # RETRIES EXHAUSTED -> DEAD LETTER
        # ---------------------------------------------------------

        return _dead_letter(source, exc)


def _dead_letter(source: dict, exc: Exception) -> dict:
    name = source["name"]

    state.mark_dead_letter(
        name,
        str(exc)
    )

    _append_dead_letter(
        source,
        str(exc)
    )

    # The job is now permanently finished
    state.mark_finished(name)
    state.release_lock(name)

    logger.error(
        "DEAD-LETTER: source '%s' failed: %s",
        name,
        exc,
    )

    return {
        "source": name,
        "status": "dead_letter",
        "error": str(exc),
    }


def _append_dead_letter(
    source: dict,
    reason: str
) -> None:

    os.makedirs(
        os.path.dirname(DEAD_LETTER_LOG),
        exist_ok=True
    )

    record = {
        "source_name": source["name"],
        "url": source["url"],
        "source_type": source["source_type"],
        "reason": reason[:1500],
        "timestamp": time.time(),
    }

    with open(
        DEAD_LETTER_LOG,
        "a",
        encoding="utf-8"
    ) as f:

        f.write(
            json.dumps(
                record,
                ensure_ascii=False
            ) + "\n"
        )
