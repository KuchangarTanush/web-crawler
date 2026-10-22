"""
Runs ONE crawl job as a clean Scrapy subprocess and reports the outcome.

Shared by the Celery task (engine.tasks) and the source checker
(python -m engine.verify_sources), so both exercise exactly the same code path.

Why a subprocess: Scrapy's Twisted reactor can only start once per process and
doesn't play well with Celery's worker model, so each job gets a clean process.
"""
import json
import os
import subprocess
import sys
import uuid
from urllib.parse import urlparse

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULT_LOG = os.path.join(PROJECT_ROOT, "crawl_output", "crawl_results.jsonl")

# Job-level retry policy (Celery layer). Shared with the scheduler so it can
# size lock lifetimes correctly.
MAX_JOB_RETRIES = 3
JOB_RETRY_BASE_DELAY = int(os.environ.get("JOB_RETRY_BASE_DELAY", 10))  # seconds; doubles each retry

# Keep in sync with Crawler/settings.py (RETRY_TIMES) - used to split a job's
# time budget across the Scrapy-level attempts.
SCRAPY_RETRY_TIMES = 3
SCRAPY_MAX_DOWNLOAD_TIMEOUT = 60

# HTTP 4xx means "the request itself is wrong" - retrying won't help.
# These 4xx codes ARE worth retrying.
TRANSIENT_4XX = {408, 425, 429}


class CrawlError(RuntimeError):
    """The crawl failed; a whole-job retry might succeed."""


class PermanentCrawlError(CrawlError):
    """The crawl failed in a way a retry cannot fix (e.g. HTTP 404/403)."""


def is_darknet(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host.endswith(".onion") or host.endswith(".i2p")


def request_timeout_for(source: dict) -> int:
    """
    Per-request download timeout, derived from the job's time budget.

    Scrapy makes up to (RETRY_TIMES + 1) attempts, each allowed DOWNLOAD_TIMEOUT
    seconds, plus ~17s of backoff sleeping. With a fixed 60s per attempt a
    60-second job could never finish its own retries: the job timeout would
    kill it first and the whole job would then be retried from scratch.
    """
    budget = source.get("timeout_seconds", 120) - 20  # leave room for backoff + startup
    per_attempt = budget // (SCRAPY_RETRY_TIMES + 1)
    floor = 30 if is_darknet(source["url"]) else 10   # Tor/I2P circuits are slow
    return int(max(floor, min(SCRAPY_MAX_DOWNLOAD_TIMEOUT, per_attempt)))


def lock_ttl_for(source: dict) -> int:
    """How long the scheduler lock must live to cover the job AND all its retries."""
    timeout = source.get("timeout_seconds", 120)
    backoff = sum(JOB_RETRY_BASE_DELAY * (2 ** i) for i in range(MAX_JOB_RETRIES))
    return int((MAX_JOB_RETRIES + 1) * (timeout + 10) + backoff + 300)


def _log_size() -> int:
    try:
        return os.path.getsize(RESULT_LOG)
    except OSError:
        return 0


def _find_record(job_id: str, offset: int) -> dict | None:
    """
    Find this job's record in crawl_results.jsonl.

    Only the bytes appended since the job started are read (not the whole,
    ever-growing file), and the record is matched by job_id instead of
    comparing timestamps.
    """
    try:
        with open(RESULT_LOG, "rb") as f:
            f.seek(offset)
            data = f.read()
    except OSError:
        return None

    needle = job_id.encode()
    found = None
    for line in data.splitlines():
        if needle not in line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("job_id") == job_id:
            found = record  # keep the last one if the job wrote several
    return found


def run_crawl(source: dict, validators: dict | None = None) -> dict:
    """
    Run one crawl and return its result record.
    Raises CrawlError (retryable) or PermanentCrawlError (not retryable).
    """
    name = source["name"]
    timeout = source.get("timeout_seconds", 120)
    job_id = uuid.uuid4().hex

    cmd = [
        # sys.executable (not the bare "scrapy" command): works even when the
        # venv isn't on PATH, and on Windows avoids the scrapy.exe launcher,
        # whose child process would survive a timeout kill as an orphan.
        sys.executable, "-m", "scrapy", "crawl", "single_source",
        "-a", f"url={source['url']}",
        "-a", f"source_name={name}",
        "-a", f"source_type={source['source_type']}",
        "-a", f"job_id={job_id}",
        "-s", f"DOWNLOAD_TIMEOUT={request_timeout_for(source)}",
    ]
    for arg, field in (("etag", "etag"), ("last_modified", "last_modified"), ("prev_hash", "prev_hash")):
        value = (validators or {}).get(field)
        if value:
            cmd += ["-a", f"{arg}={value}"]

    env = dict(os.environ, PYTHONUTF8="1", PYTHONIOENCODING="utf-8")
    offset = _log_size()

    try:
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired as exc:
        raise CrawlError(f"crawl for '{name}' exceeded its {timeout}s timeout") from exc

    if result.returncode != 0:
        raise CrawlError(
            f"scrapy crawl exited {result.returncode} for source '{name}': "
            f"{result.stderr[-1500:]}"
        )

    # Scrapy exits 0 even when the fetch failed, so read the spider's own verdict.
    record = _find_record(job_id, offset)

    if record is None:
        raise CrawlError(
            f"scrapy crawl for '{name}' exited cleanly but produced no "
            f"crawl-result record — treating as a failed fetch"
        )

    if record.get("status") != "success":
        message = f"fetch failed for '{name}': {record.get('error') or 'unknown error'}"
        http_status = record.get("http_status")
        if http_status and 400 <= http_status < 500 and http_status not in TRANSIENT_4XX:
            raise PermanentCrawlError(message)
        raise CrawlError(message)

    return record
