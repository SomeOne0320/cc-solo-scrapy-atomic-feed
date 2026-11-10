"""Tests for scrapy.core.sqlite_scheduler.SQLiteScheduler.

All tests use real, temporary JOBDIR directories. Crash-recovery scenarios
are exercised both in-process (close/reopen) and with child processes that
die abruptly (os._exit / SIGKILL) without closing the scheduler.
"""

from __future__ import annotations

import multiprocessing
import os
import shutil
import sqlite3
import tempfile

import pytest
from twisted.internet import defer
from twisted.trial.unittest import TestCase

from scrapy.core.scheduler import Scheduler
from scrapy.core.sqlite_scheduler import SQLiteScheduler
from scrapy.http import Request
from scrapy.spiders import Spider
from scrapy.utils.test import get_crawler
from tests.mockserver import MockServer

SCHEDULER_PATH = "scrapy.core.sqlite_scheduler.SQLiteScheduler"


def make_crawler(jobdir, **settings):
    settings_dict = {
        "JOBDIR": str(jobdir),
        "SCHEDULER": SCHEDULER_PATH,
        **settings,
    }
    return get_crawler(Spider, settings_dict)


def open_scheduler(jobdir, spider=None, **settings):
    crawler = make_crawler(jobdir, **settings)
    scheduler = SQLiteScheduler.from_crawler(crawler)
    scheduler.open(spider or Spider(name="spider"))
    return scheduler


def dispatch_all(scheduler):
    """Dispatch every pending request, returning them in dispatch order."""
    requests = []
    while scheduler.has_pending_requests():
        request = scheduler.next_request()
        assert request is not None
        requests.append(request)
    assert scheduler.next_request() is None
    return requests


# ---------------------------------------------------------------------------
# Child-process entry points (must be module level for multiprocessing).
# Each child simulates a sudden crash with os._exit(): no close(), no atexit.
# ---------------------------------------------------------------------------


def _child_enqueue_and_die(jobdir, url, conn):
    scheduler = open_scheduler(jobdir)
    scheduler.enqueue_request(Request(url))
    conn.send("enqueued")
    conn.close()
    os._exit(0)


def _child_claim_and_die(jobdir, conn):
    scheduler = open_scheduler(jobdir)
    request = scheduler.next_request()
    conn.send(request.url if request is not None else None)
    conn.close()
    os._exit(0)


def _child_claim_ack_and_die(jobdir, conn):
    scheduler = open_scheduler(jobdir)
    request = scheduler.next_request()
    acked = scheduler.ack_request(request) if request is not None else None
    conn.send((request.url if request is not None else None, acked))
    conn.close()
    os._exit(0)


def _child_hold_lock(jobdir, conn):
    scheduler = open_scheduler(jobdir)  # noqa: F841  # holds the JOBDIR lock
    conn.send("opened")
    # Wait until the parent kills us (SIGKILL): the scheduler is never closed.
    conn.recv()


def run_child(target, args):
    """Run *target* in a child process, return (exitcode, received value)."""
    ctx = multiprocessing.get_context("fork")
    parent_conn, child_conn = ctx.Pipe(duplex=True)
    process = ctx.Process(target=target, args=(*args, child_conn))
    process.start()
    received = parent_conn.recv()
    process.join(timeout=30)
    if process.is_alive():
        process.kill()
        process.join()
        raise AssertionError("child process did not exit")
    parent_conn.close()
    return process.exitcode, received


class TestEnqueueAndDispatch:
    def test_priority_and_fifo_order(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        # (url, priority): same priority must dispatch in FIFO (enqueue) order
        enqueued = [
            ("http://example.com/a", 0),
            ("http://example.com/b", 5),
            ("http://example.com/c", 0),
            ("http://example.com/d", -1),
            ("http://example.com/e", 5),
            ("http://example.com/f", 0),
        ]
        for url, priority in enqueued:
            assert scheduler.enqueue_request(Request(url, priority=priority))
        dispatched = dispatch_all(scheduler)
        assert [r.url for r in dispatched] == [
            "http://example.com/b",  # priority 5, enqueued first
            "http://example.com/e",  # priority 5, enqueued second
            "http://example.com/a",  # priority 0, FIFO
            "http://example.com/c",
            "http://example.com/f",
            "http://example.com/d",  # lowest priority last
        ]
        scheduler.close("finished")

    def test_fifo_order_survives_restart(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        for letter in "abc":
            scheduler.enqueue_request(Request(f"http://example.com/{letter}"))
        scheduler.close("finished")
        scheduler = open_scheduler(tmp_path)
        dispatched = dispatch_all(scheduler)
        assert [r.url for r in dispatched] == [
            f"http://example.com/{letter}" for letter in "abc"
        ]
        scheduler.close("finished")

    def test_request_attributes_survive_serialization(self, tmp_path):
        spider = Spider(name="spider")
        scheduler = open_scheduler(tmp_path, spider=spider)
        scheduler.enqueue_request(
            Request(
                "http://example.com/a",
                callback=spider.parse,
                method="POST",
                body=b"payload",
                headers={"X-Test": "1"},
                meta={"key": "value"},
                priority=3,
            )
        )
        (request,) = dispatch_all(scheduler)
        assert request.url == "http://example.com/a"
        assert request.callback.__name__ == "parse"
        assert request.method == "POST"
        assert request.body == b"payload"
        assert request.headers["X-Test"] == b"1"
        assert request.meta["key"] == "value"
        assert request.priority == 3
        scheduler.close("finished")

    def test_len_and_has_pending(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        assert not scheduler.has_pending_requests()
        assert len(scheduler) == 0
        scheduler.enqueue_request(Request("http://example.com/a"))
        scheduler.enqueue_request(Request("http://example.com/b"))
        assert scheduler.has_pending_requests()
        assert len(scheduler) == 2
        scheduler.next_request()
        assert len(scheduler) == 1  # in-flight requests are not pending
        scheduler.close("finished")


class TestDeduplication:
    def test_duplicate_fingerprint_rejected(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        assert scheduler.enqueue_request(Request("http://example.com/a"))
        assert not scheduler.enqueue_request(Request("http://example.com/a"))
        assert len(scheduler) == 1
        assert (
            scheduler.crawler.stats.get_value("sqlite_scheduler/dupefilter_filtered")
            == 1
        )
        scheduler.close("finished")

    def test_dont_filter_requests_get_independent_records(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        assert scheduler.enqueue_request(Request("http://example.com/a"))
        # dont_filter requests are never deduplicated, each is persisted
        assert scheduler.enqueue_request(
            Request("http://example.com/a", dont_filter=True)
        )
        assert scheduler.enqueue_request(
            Request("http://example.com/a", dont_filter=True)
        )
        dispatched = dispatch_all(scheduler)
        assert [r.url for r in dispatched] == ["http://example.com/a"] * 3
        scheduler.close("finished")

    def test_fingerprint_rules_match_default_dupefilter(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        # Same canonical URL (query parameter order) -> same fingerprint
        assert scheduler.enqueue_request(Request("http://example.com/a?x=1&y=2"))
        assert not scheduler.enqueue_request(Request("http://example.com/a?y=2&x=1"))
        # Different method -> different fingerprint
        assert scheduler.enqueue_request(
            Request("http://example.com/a?y=2&x=1", method="POST")
        )
        scheduler.close("finished")

    def test_dedup_survives_ack_and_restart(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        scheduler.enqueue_request(Request("http://example.com/a"))
        (request,) = dispatch_all(scheduler)
        assert scheduler.ack_request(request)
        # The acknowledged request is gone but its fingerprint still filters.
        assert not scheduler.enqueue_request(Request("http://example.com/a"))
        scheduler.close("finished")
        scheduler = open_scheduler(tmp_path)
        assert not scheduler.enqueue_request(Request("http://example.com/a"))
        assert not scheduler.has_pending_requests()
        scheduler.close("finished")

    def test_base_dupefilter_disables_dedup(self, tmp_path):
        scheduler = open_scheduler(
            tmp_path, DUPEFILTER_CLASS="scrapy.dupefilters.BaseDupeFilter"
        )
        assert scheduler.enqueue_request(Request("http://example.com/a"))
        assert scheduler.enqueue_request(Request("http://example.com/a"))
        assert len(scheduler) == 2
        scheduler.close("finished")


class TestEnqueueTransaction:
    """A failed enqueue transaction must not leave a fingerprint behind."""

    class FailingConnection:
        """Proxy that fails the Nth execute() call, simulating a
        mid-transaction storage failure."""

        def __init__(self, real, fail_on_call):
            self._real = real
            self._fail_on_call = fail_on_call
            self.calls = 0

        def execute(self, *args, **kwargs):
            self.calls += 1
            if self.calls == self._fail_on_call:
                raise sqlite3.OperationalError("simulated mid-transaction failure")
            return self._real.execute(*args, **kwargs)

        def __enter__(self):
            self._real.__enter__()
            return self

        def __exit__(self, *args):
            return self._real.__exit__(*args)

    @pytest.mark.parametrize("fail_on_call", [1, 2])
    def test_failed_enqueue_transaction_allows_retry(self, tmp_path, fail_on_call):
        scheduler = open_scheduler(tmp_path)
        real_conn = scheduler._conn
        scheduler._conn = self.FailingConnection(real_conn, fail_on_call)
        with pytest.raises(sqlite3.OperationalError):
            scheduler.enqueue_request(Request("http://example.com/a"))
        scheduler._conn = real_conn
        # The rolled-back transaction left no fingerprint: the same request
        # can be enqueued again.
        assert scheduler.enqueue_request(Request("http://example.com/a"))
        assert len(scheduler) == 1
        scheduler.close("finished")

    def test_unserializable_request_rejected_without_fingerprint(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        # A lambda callback cannot be resolved to a spider method.
        with pytest.raises(ValueError, match="Unable to serialize request"):
            scheduler.enqueue_request(
                Request("http://example.com/a", callback=lambda r: None)
            )
        # Unserializable meta values cannot be pickled.
        with pytest.raises(ValueError, match="Unable to serialize request"):
            scheduler.enqueue_request(
                Request("http://example.com/b", meta={"obj": lambda: None})
            )
        stats = scheduler.crawler.stats
        assert stats.get_value("sqlite_scheduler/unserializable") == 2
        # No fingerprints were recorded: both URLs can be enqueued once fixed.
        assert scheduler.enqueue_request(Request("http://example.com/a"))
        assert scheduler.enqueue_request(Request("http://example.com/b"))
        assert len(scheduler) == 2
        scheduler.close("finished")


class TestAcknowledgement:
    def test_ack_is_idempotent(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        scheduler.enqueue_request(Request("http://example.com/a"))
        (request,) = dispatch_all(scheduler)
        assert scheduler.ack_request(request) is True
        # A duplicate acknowledgement is a no-op, not an error.
        assert scheduler.ack_request(request) is False
        stats = scheduler.crawler.stats
        assert stats.get_value("sqlite_scheduler/acked") == 1
        assert stats.get_value("sqlite_scheduler/ack_stale") == 1
        scheduler.close("finished")

    def test_ack_without_token_rejected(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        with pytest.raises(ValueError, match="no dispatch token"):
            scheduler.ack_request(Request("http://example.com/never-dispatched"))
        scheduler.close("finished")

    def test_unacked_request_redispatched_after_restart(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        scheduler.enqueue_request(Request("http://example.com/a"))
        (first,) = dispatch_all(scheduler)
        first_token = first.meta[SQLiteScheduler.ACK_META_KEY]
        scheduler.close("finished")
        scheduler = open_scheduler(tmp_path)
        assert scheduler.crawler.stats.get_value("sqlite_scheduler/recovered") == 1
        (second,) = dispatch_all(scheduler)
        assert second.url == first.url
        # A new dispatch gets a new token.
        assert second.meta[SQLiteScheduler.ACK_META_KEY] != first_token
        scheduler.close("finished")

    def test_acked_request_not_redispatched_after_restart(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        scheduler.enqueue_request(Request("http://example.com/a"))
        scheduler.enqueue_request(Request("http://example.com/b"))
        first, second = dispatch_all(scheduler)
        assert scheduler.ack_request(first)
        scheduler.close("finished")
        scheduler = open_scheduler(tmp_path)
        (redispatched,) = dispatch_all(scheduler)
        assert redispatched.url == second.url
        scheduler.close("finished")

    def test_stale_token_cannot_ack_redispatched_request(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        scheduler.enqueue_request(Request("http://example.com/a"))
        (first,) = dispatch_all(scheduler)
        scheduler.close("finished")

        scheduler = open_scheduler(tmp_path)
        (redispatched,) = dispatch_all(scheduler)
        assert redispatched.url == first.url
        # The token from the previous run must not acknowledge the
        # re-dispatched request.
        assert scheduler.ack_request(first) is False
        assert scheduler.ack_request(redispatched) is True
        scheduler.close("finished")

        scheduler = open_scheduler(tmp_path)
        assert not scheduler.has_pending_requests()
        scheduler.close("finished")

    def test_dispatch_token_not_persisted_on_reenqueue(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        scheduler.enqueue_request(Request("http://example.com/a", dont_filter=True))
        first = scheduler.next_request()
        first_token = first.meta[SQLiteScheduler.ACK_META_KEY]
        # Re-enqueueing a dispatched request object (which carries a dispatch
        # token in its meta) must not persist that token.
        assert scheduler.enqueue_request(first)
        second = scheduler.next_request()
        assert second.meta[SQLiteScheduler.ACK_META_KEY] != first_token
        # The new dispatch is acknowledged with its own token.
        assert scheduler.ack_request(second) is True
        scheduler.close("finished")


class TestProcessRecovery:
    def test_child_enqueue_then_crash_parent_recovers(self, tmp_path):
        exitcode, received = run_child(
            _child_enqueue_and_die, (str(tmp_path), "http://example.com/a")
        )
        assert received == "enqueued"
        assert exitcode == 0
        # The enqueue was committed before the crash: it is recoverable.
        scheduler = open_scheduler(tmp_path)
        (request,) = dispatch_all(scheduler)
        assert request.url == "http://example.com/a"
        scheduler.close("finished")

    def test_child_claims_then_crashes_parent_redispatches(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        scheduler.enqueue_request(Request("http://example.com/a"))
        scheduler.enqueue_request(Request("http://example.com/b"))
        scheduler.close("finished")

        exitcode, claimed_url = run_child(_child_claim_and_die, (str(tmp_path),))
        assert exitcode == 0
        assert claimed_url == "http://example.com/a"

        # The child died without closing the scheduler; a new process takes
        # over and the claimed-but-unacknowledged request is dispatched again.
        scheduler = open_scheduler(tmp_path)
        dispatched = dispatch_all(scheduler)
        assert [r.url for r in dispatched] == [
            "http://example.com/a",
            "http://example.com/b",
        ]
        scheduler.close("finished")

    def test_child_acks_then_crashes_no_redispatch(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        scheduler.enqueue_request(Request("http://example.com/a"))
        scheduler.close("finished")

        exitcode, (url, acked) = run_child(_child_claim_ack_and_die, (str(tmp_path),))
        assert exitcode == 0
        assert (url, acked) == ("http://example.com/a", True)

        # The acknowledgement was committed before the crash: the request is
        # never dispatched again, but its fingerprint still filters duplicates.
        scheduler = open_scheduler(tmp_path)
        assert not scheduler.has_pending_requests()
        assert not scheduler.enqueue_request(Request("http://example.com/a"))
        scheduler.close("finished")

    def test_exclusive_access_and_takeover_after_kill(self, tmp_path):
        ctx = multiprocessing.get_context("fork")
        parent_conn, child_conn = ctx.Pipe(duplex=True)
        process = ctx.Process(target=_child_hold_lock, args=(str(tmp_path), child_conn))
        process.start()
        assert parent_conn.recv() == "opened"
        try:
            # While the child holds the scheduler, no other process may open
            # a scheduler on the same JOBDIR.
            with pytest.raises(RuntimeError, match="locked by another active"):
                open_scheduler(tmp_path)
        finally:
            process.kill()  # simulate an abnormal exit
            process.join(timeout=30)
        parent_conn.close()
        assert process.exitcode != 0
        # The OS released the lock when the child died: take over.
        scheduler = open_scheduler(tmp_path)
        scheduler.close("finished")

    def test_exclusive_access_same_process(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        with pytest.raises(RuntimeError, match="locked by another active"):
            open_scheduler(tmp_path)
        scheduler.close("finished")
        # The lock is released on close: the JOBDIR can be reused.
        scheduler = open_scheduler(tmp_path)
        scheduler.close("finished")


class TestConfiguration:
    def test_missing_jobdir_rejected(self):
        crawler = get_crawler(Spider, {"SCHEDULER": SCHEDULER_PATH})
        with pytest.raises(ValueError, match="requires the JOBDIR setting"):
            SQLiteScheduler.from_crawler(crawler)
        with pytest.raises(ValueError, match="requires a jobdir"):
            SQLiteScheduler(jobdir=None)

    def test_custom_dupefilter_rejected(self, tmp_path):
        crawler = make_crawler(
            tmp_path, DUPEFILTER_CLASS="scrapy.pqueues.ScrapyPriorityQueue"
        )
        with pytest.raises(ValueError, match="does not support DUPEFILTER_CLASS"):
            SQLiteScheduler.from_crawler(crawler)

    def test_custom_queue_settings_warn(self, tmp_path, caplog):
        crawler = make_crawler(
            tmp_path, SCHEDULER_DISK_QUEUE="scrapy.squeues.PickleFifoDiskQueue"
        )
        with caplog.at_level("WARNING"):
            SQLiteScheduler.from_crawler(crawler)
        assert "SCHEDULER_DISK_QUEUE" in caplog.text
        assert "does not support custom queue classes" in caplog.text

    def test_use_before_open_and_after_close_rejected(self, tmp_path):
        crawler = make_crawler(tmp_path)
        scheduler = SQLiteScheduler.from_crawler(crawler)
        with pytest.raises(RuntimeError, match="is not open"):
            scheduler.enqueue_request(Request("http://example.com/a"))
        with pytest.raises(RuntimeError, match="is not open"):
            scheduler.next_request()
        scheduler.open(Spider(name="spider"))
        scheduler.enqueue_request(Request("http://example.com/a"))
        (request,) = dispatch_all(scheduler)
        scheduler.close("finished")
        with pytest.raises(RuntimeError, match="is not open"):
            scheduler.ack_request(request)
        # Closing twice is harmless.
        scheduler.close("finished")

    def test_storage_files(self, tmp_path):
        scheduler = open_scheduler(tmp_path)
        scheduler.enqueue_request(Request("http://example.com/a"))
        scheduler.close("finished")
        assert (tmp_path / SQLiteScheduler.DATABASE_FILENAME).exists()
        assert (tmp_path / SQLiteScheduler.LOCK_FILENAME).exists()
        # The default scheduler's artifacts are not created.
        assert not (tmp_path / "requests.queue").exists()
        assert not (tmp_path / "requests.seen").exists()


class TestDefaultSchedulerCompatibility:
    """The default scheduler keeps working unchanged, on the same JOBDIR."""

    def test_default_scheduler_still_works(self, tmp_path):
        crawler = make_crawler(tmp_path, SCHEDULER="scrapy.core.scheduler.Scheduler")
        scheduler = Scheduler.from_crawler(crawler)
        spider = Spider(name="spider")
        spider.crawler = crawler
        scheduler.open(spider)
        assert scheduler.enqueue_request(Request("http://example.com/a"))
        assert not scheduler.enqueue_request(Request("http://example.com/a"))
        request = scheduler.next_request()
        assert request.url == "http://example.com/a"
        scheduler.close("finished")
        # The default scheduler uses its own on-disk layout, untouched by
        # SQLiteScheduler, and vice versa.
        assert (tmp_path / "requests.queue").exists()
        assert not (tmp_path / SQLiteScheduler.DATABASE_FILENAME).exists()

    def test_default_scheduler_selected_by_default(self):
        from scrapy.utils.misc import load_object

        crawler = get_crawler(Spider, {})
        assert load_object(crawler.settings["SCHEDULER"]) is Scheduler


# ---------------------------------------------------------------------------
# Engine-level integration tests over a real local HTTP server.
# ---------------------------------------------------------------------------


class AckSpider(Spider):
    """A spider that acknowledges each request from business code (its
    callback) after 'writing its output', via the public engine API."""

    name = "ack_spider"

    def __init__(self, start_urls, **kwargs):
        self.start_urls = start_urls
        super().__init__(name=self.name, **kwargs)

    def parse(self, response):
        scheduler = self.crawler.engine.scheduler
        assert isinstance(scheduler, SQLiteScheduler)
        assert scheduler.ack_request(response.request)
        yield {"url": response.url}


class NoAckSpider(Spider):
    """A spider that never acknowledges its requests."""

    name = "no_ack_spider"

    def __init__(self, start_urls, **kwargs):
        self.start_urls = start_urls
        super().__init__(name=self.name, **kwargs)

    def parse(self, response):
        yield {"url": response.url}


class TestEngineIntegration(TestCase):
    def setUp(self):
        self.jobdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.jobdir, True)

    def _settings(self):
        return {"SCHEDULER": SCHEDULER_PATH, "JOBDIR": self.jobdir}

    @defer.inlineCallbacks
    def test_crawl_with_business_ack(self):
        with MockServer() as mockserver:
            urls = [
                mockserver.url(f"/status?n=200&i={i}", is_secure=False)
                for i in range(4)
            ]
            crawler = get_crawler(AckSpider, self._settings())
            yield crawler.crawl(urls)
            stats = crawler.stats
            assert stats.get_value("scheduler/enqueued") == len(urls)
            assert stats.get_value("scheduler/dequeued") == len(urls)
            assert stats.get_value("sqlite_scheduler/acked") == len(urls)
            assert stats.get_value("item_scraped_count") == len(urls)
            # Everything was acknowledged: nothing is left for a next run.
            assert stats.get_value("sqlite_scheduler/pending") == 0
            assert stats.get_value("sqlite_scheduler/inflight") == 0
            yield crawler.stop()

    @defer.inlineCallbacks
    def test_unacked_requests_recrawled_on_next_run(self):
        with MockServer() as mockserver:
            urls = [
                mockserver.url(f"/status?n=200&i={i}", is_secure=False)
                for i in range(3)
            ]
            crawler = get_crawler(NoAckSpider, self._settings())
            yield crawler.crawl(urls)
            assert crawler.stats.get_value("downloader/response_count") == len(urls)
            # Nothing was acknowledged: all requests remain in-flight.
            assert crawler.stats.get_value("sqlite_scheduler/inflight") == len(urls)
            yield crawler.stop()

            # A new crawl on the same JOBDIR recovers and re-dispatches them
            # (at-least-once delivery).
            crawler2 = get_crawler(NoAckSpider, self._settings())
            yield crawler2.crawl([])  # no start requests needed
            assert crawler2.stats.get_value("sqlite_scheduler/recovered") == len(urls)
            assert crawler2.stats.get_value("downloader/response_count") == len(urls)
            yield crawler2.stop()
