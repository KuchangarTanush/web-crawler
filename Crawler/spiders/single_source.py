"""
Single-source spider.

Scope, deliberately: start from one authorized seed URL, fetch it through the
right gateway (direct / Tor / I2P, handled by ProxyRoutingMiddleware), parse
it, and produce exactly one crawl-result record. No link-following, no
scheduling, no multi-source queueing — that is the engine layer's job.

Usage:
    scrapy crawl single_source \
        -a url="http://example.onion/page" \
        -a source_name="source_1" \
        -a source_type="onion_site"

Optional arguments (the engine passes these; you rarely type them by hand):
    -a job_id=<token>          echoed into the result record
    -a etag=<value>            sent as If-None-Match
    -a last_modified=<value>   sent as If-Modified-Since
    -a prev_hash=<sha256>      content hash from the previous successful crawl,
                               reused when the server answers 304

IMPORTANT: this spider must keep using the async `start()` entry point.
With the legacy `start_requests()` entry point, Scrapy 2.19 never calls the
errback for HTTP errors (404, 403, ...), so no "failed" record is written.
"""
from datetime import datetime, timezone

import scrapy
from scrapy.http import TextResponse

from Crawler.items import CrawlResultItem


def _header(response, name):
    value = response.headers.get(name)
    return value.decode("latin-1") if value else None


class SingleSourceSpider(scrapy.Spider):
    name = "single_source"

    custom_settings = {
        # Per-source limit: this spider only ever touches one host, so keep
        # it polite regardless of the global CONCURRENT_REQUESTS setting.
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
    }

    def __init__(
        self,
        url=None,
        source_name="source_1",
        source_type="normal",
        job_id=None,
        etag=None,
        last_modified=None,
        prev_hash=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if not url:
            raise ValueError("Provide the authorized seed URL: -a url=<...>")
        self.start_urls = [url]
        self.source_name = source_name
        self.source_type = source_type
        self.job_id = job_id or None
        self.etag = etag or None
        self.last_modified = last_modified or None
        self.prev_hash = prev_hash or None

    # ------------------------------------------------------------------ #
    # Requests
    # ------------------------------------------------------------------ #
    async def start(self):
        for url in self.start_urls:
            yield self._build_request(url)

    def _build_request(self, url):
        headers = {}
        # Conditional GET: if nothing changed the server replies 304 with no
        # body, which saves bandwidth, parsing, hashing and evidence storage.
        if self.etag:
            headers["If-None-Match"] = self.etag
        if self.last_modified:
            headers["If-Modified-Since"] = self.last_modified

        return scrapy.Request(
            url,
            callback=self.parse,
            errback=self.handle_failure,
            headers=headers,
            dont_filter=True,
            # 304 is not an error for us; let it reach parse().
            meta={"handle_httpstatus_list": [304]},
        )

    # ------------------------------------------------------------------ #
    # Results
    # ------------------------------------------------------------------ #
    def _base_item(self, url, meta, status):
        item = CrawlResultItem()
        item["source_name"] = self.source_name
        item["source_type"] = self.source_type
        item["job_id"] = self.job_id
        item["url"] = url
        item["network"] = meta.get("network", "clearnet")
        item["fetch_timestamp"] = datetime.now(timezone.utc).isoformat()
        item["crawler_build"] = self.settings.get("CRAWLER_BUILD")
        item["status"] = status
        item["attempt"] = meta.get("retry_times", 0) + 1
        item["unchanged"] = False
        item["error"] = None
        item["title"] = ""
        item["raw_html"] = ""
        item["text_content"] = ""
        return item

    def parse(self, response):
        item = self._base_item(response.url, response.meta, "success")
        item["http_status"] = response.status

        # 304 Not Modified: nothing new to store.
        if response.status == 304:
            item["unchanged"] = True
            item["content_hash"] = self.prev_hash
            item["etag"] = self.etag
            item["last_modified"] = self.last_modified
            yield item
            return

        item["etag"] = _header(response, "ETag")
        item["last_modified"] = _header(response, "Last-Modified")

        # Binary responses (PDF, images, archives) have no .xpath/.text.
        if not isinstance(response, TextResponse):
            item["status"] = "failed"
            item["error"] = (
                f"unsupported content type: {_header(response, 'Content-Type')}"
            )
            yield item
            return

        item["title"] = response.xpath("//title/text()").get(default="").strip()
        item["raw_html"] = response.text
        item["text_content"] = " ".join(
            t.strip() for t in response.xpath("//body//text()").getall() if t.strip()
        )

        # Empty page: HTTP 200 but nothing to collect (blank body, challenge
        # stub, soft block). Count it as a failure so it is retried, backs the
        # source off, and never produces a "success" record with no content.
        if not item["text_content"].strip():
            item["status"] = "failed"
            item["error"] = "empty page (no visible text)"
            item["raw_html"] = ""
            item["text_content"] = ""
        yield item

    def handle_failure(self, failure):
        request = failure.request
        response = getattr(failure.value, "response", None)
        http_status = response.status if response is not None else None
        error = f"HTTP {http_status}" if http_status else (
            str(failure.value) or failure.type.__name__
        )
        self.logger.error("Fetch failed permanently for %s: %s", request.url, error)

        item = self._base_item(request.url, request.meta, "failed")
        item["http_status"] = http_status
        item["error"] = error[:500]
        yield item
