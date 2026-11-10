"""
SQLite-backed recoverable scheduler.

This module provides :class:`SQLiteScheduler`, an optional, opt-in scheduler
(selected through the :setting:`SCHEDULER` setting) that stores both the
request queue and the duplicate-filter fingerprints in a single SQLite
database inside :setting:`JOBDIR`, committing them in the same transaction.

Unlike the default scheduler (:class:`scrapy.core.scheduler.Scheduler`), a
request that has been dispatched is *not* removed from storage; it is marked
as in-flight until the calling code explicitly acknowledges it with
:meth:`SQLiteScheduler.ack_request`. Requests that are never acknowledged are
dispatched again on the next run (at-least-once delivery).
"""

from __future__ import annotations

import logging
import os
import pickle
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

from scrapy.core.scheduler import BaseScheduler
from scrapy.dupefilters import BaseDupeFilter, RFPDupeFilter
from scrapy.settings import default_settings
from scrapy.spiders import Spider  # noqa: TC001
from scrapy.utils.job import job_dir
from scrapy.utils.misc import load_object
from scrapy.utils.request import RequestFingerprinter, request_from_dict

if TYPE_CHECKING:
    # typing.Self requires Python 3.11
    from typing_extensions import Self

    from scrapy.crawler import Crawler
    from scrapy.http.request import Request
    from scrapy.statscollectors import StatsCollector
    from scrapy.utils.request import RequestFingerprinterProtocol


logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS fingerprints (
    fingerprint TEXT PRIMARY KEY,
    created REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT,
    priority INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'inflight')),
    payload BLOB NOT NULL,
    dispatch_token TEXT,
    enqueued_at REAL NOT NULL,
    dispatched_at REAL
);
CREATE INDEX IF NOT EXISTS requests_dispatch_idx
    ON requests (state, priority DESC, id);
"""

#: Settings that the default scheduler supports but SQLiteScheduler ignores,
#: because it manages its own SQLite storage instead of queue classes.
_IGNORED_QUEUE_SETTINGS = (
    "SCHEDULER_PRIORITY_QUEUE",
    "SCHEDULER_DISK_QUEUE",
    "SCHEDULER_MEMORY_QUEUE",
    "SCHEDULER_START_DISK_QUEUE",
    "SCHEDULER_START_MEMORY_QUEUE",
)


class SQLiteScheduler(BaseScheduler):
    """SQLite-backed recoverable scheduler with at-least-once delivery.

    Enable it with::

        SCHEDULER = "scrapy.core.sqlite_scheduler.SQLiteScheduler"
        JOBDIR = "crawls/mycrawl"   # mandatory

    Storage
    =======

    All state lives in ``<JOBDIR>/scheduler.sqlite``. Enqueuing a request
    records its :ref:`fingerprint <request-fingerprints>` (unless
    :attr:`~scrapy.Request.dont_filter` is ``True``) and its serialized
    payload in a **single SQLite transaction**, so a committed enqueue is
    always fully recoverable after a crash, and a failed enqueue (e.g. a
    serialization error or a transaction rollback) never leaves a fingerprint
    behind that would block a retry.

    Requests are dispatched by descending
    :attr:`~scrapy.Request.priority`; requests with the same priority are
    dispatched in FIFO (enqueue) order.

    Delivery and acknowledgement
    ============================

    :meth:`next_request` marks the dispatched request as ``inflight`` instead
    of deleting it, and hands the caller a dispatch token (stored in
    ``request.meta[SQLiteScheduler.ACK_META_KEY]``). The request stays
    in-flight until business code explicitly confirms, with
    :meth:`ack_request`, that its own output has been written. HTTP success,
    the callback returning, or items passing through pipelines do **not**
    acknowledge a request.

    Acknowledgements are idempotent and bound to the dispatch token of the
    current dispatch: acknowledging twice is a no-op, and a token from a
    previous run (e.g. a late acknowledgement after a restart) can never
    acknowledge a re-dispatched request.

    On :meth:`open`, any request left in-flight by a previous run (crash,
    kill, unclosed spider) is moved back to ``pending`` and will be
    dispatched again. Acknowledged requests are never dispatched again, but
    their fingerprint records are kept, so they are still rejected as
    duplicates if they are enqueued again.

    Because delivery is at-least-once, business output and the local
    acknowledgement are not the same transaction: make your outputs
    idempotent (e.g. keyed by a business idempotency key) to tolerate the
    rare duplicate delivery. See :ref:`topics-scheduler-sqlite` for a
    complete example.

    Concurrency
    ===========

    A ``JOBDIR`` may only be used by one active ``SQLiteScheduler`` at a
    time; an exclusive, OS-level lock file
    (``<JOBDIR>/scheduler.sqlite.lock``) is held while the scheduler is open.
    If the process dies, the OS releases the lock and a new process can take
    over the same ``JOBDIR``.

    Settings compatibility
    ======================

    * :setting:`JOBDIR` is **required**; without it, initialization raises
      :exc:`ValueError` instead of silently degrading to in-memory queues.
    * Requests that cannot be serialized are rejected with :exc:`ValueError`;
      there is no silent fallback to a memory queue.
    * :setting:`DUPEFILTER_CLASS`: deduplication is built into this scheduler
      (so that fingerprints share the enqueue transaction). The default
      ``scrapy.dupefilters.RFPDupeFilter`` value enables it,
      ``scrapy.dupefilters.BaseDupeFilter`` disables it, and any other value
      raises :exc:`ValueError`. :setting:`REQUEST_FINGERPRINTER_CLASS` is
      honored either way.
    * :setting:`SCHEDULER_PRIORITY_QUEUE`, :setting:`SCHEDULER_DISK_QUEUE`,
      :setting:`SCHEDULER_MEMORY_QUEUE`, :setting:`SCHEDULER_START_DISK_QUEUE`
      and :setting:`SCHEDULER_START_MEMORY_QUEUE` are not supported (storage
      is SQLite, not queuelib); non-default values are ignored with a
      warning.
    """

    #: Key of :attr:`request.meta <scrapy.Request.meta>` under which
    #: :meth:`next_request` stores the ``(row id, dispatch token)`` pair that
    #: :meth:`ack_request` needs. It is set on the dispatched request object
    #: only; it is never persisted into newly enqueued requests.
    ACK_META_KEY = "sqlite_scheduler.ack"

    #: Name of the SQLite database file, inside :setting:`JOBDIR`.
    DATABASE_FILENAME = "scheduler.sqlite"

    #: Name of the lock file, inside :setting:`JOBDIR`.
    LOCK_FILENAME = "scheduler.sqlite.lock"

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        settings = crawler.settings
        jobdir = job_dir(settings)
        if not jobdir:
            raise ValueError(
                f"{cls.__name__} requires the JOBDIR setting to point to a "
                "writable directory; it does not support in-memory operation."
            )
        dupefilter_cls = load_object(settings["DUPEFILTER_CLASS"])
        if dupefilter_cls is RFPDupeFilter:
            dedup_enabled = True
        elif dupefilter_cls is BaseDupeFilter:
            dedup_enabled = False
        else:
            raise ValueError(
                f"{cls.__name__} does not support DUPEFILTER_CLASS "
                f"{settings['DUPEFILTER_CLASS']!r}: duplicate filtering is "
                "built into its SQLite storage so that a fingerprint and its "
                "request are recorded in the same transaction. Set "
                "DUPEFILTER_CLASS to 'scrapy.dupefilters.RFPDupeFilter' (the "
                "default) to enable deduplication, or to "
                "'scrapy.dupefilters.BaseDupeFilter' to disable it. "
                "REQUEST_FINGERPRINTER_CLASS is honored either way."
            )
        ignored = [
            name
            for name in _IGNORED_QUEUE_SETTINGS
            if load_object(settings[name])
            is not load_object(getattr(default_settings, name))
        ]
        if ignored:
            logger.warning(
                "%s manages its own SQLite storage and does not support "
                "custom queue classes; ignoring setting(s): %s",
                cls.__name__,
                ", ".join(ignored),
            )
        return cls(
            jobdir=jobdir,
            stats=crawler.stats,
            crawler=crawler,
            dedup_enabled=dedup_enabled,
            fingerprinter=crawler.request_fingerprinter,
        )

    def __init__(
        self,
        *,
        jobdir: str | None,
        stats: StatsCollector | None = None,
        crawler: Crawler | None = None,
        dedup_enabled: bool = True,
        fingerprinter: RequestFingerprinterProtocol | None = None,
    ) -> None:
        if not jobdir:
            raise ValueError(
                f"{type(self).__name__} requires a jobdir; it does not "
                "support in-memory operation."
            )
        self.jobdir: str = jobdir
        self.stats: StatsCollector | None = stats
        self.crawler: Crawler | None = crawler
        self.dedup_enabled: bool = dedup_enabled
        self._fingerprinter: RequestFingerprinterProtocol = (
            fingerprinter or RequestFingerprinter()
        )
        self.spider: Spider | None = None
        self._conn: sqlite3.Connection | None = None
        self._lock_file: Any = None
        self._mutex = threading.RLock()
        self._opened = False

    # ------------------------------------------------------------------ open

    def open(self, spider: Spider) -> None:
        """Acquire the JOBDIR lock, open the database and recover state.

        Requests left in-flight by a previous run are moved back to
        ``pending`` so that they are dispatched again.
        """
        if self._opened:
            raise RuntimeError(f"{type(self).__name__} is already open")
        self.spider = spider
        self._acquire_lock()
        try:
            db_path = Path(self.jobdir, self.DATABASE_FILENAME)
            self._conn = sqlite3.connect(
                str(db_path), timeout=30, check_same_thread=False
            )
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA busy_timeout=30000")
            self._conn.executescript(_SCHEMA)
            recovered = self._recover_inflight()
        except Exception:
            logger.exception(
                "Failed to open the SQLite scheduler database in %(jobdir)r",
                {"jobdir": self.jobdir},
                extra={"spider": spider},
            )
            self._close_connection()
            self._release_lock()
            raise
        self._opened = True
        if self.stats is not None:
            self.stats.set_value("sqlite_scheduler/recovered", recovered, spider=spider)
        pending = self._count("pending")
        logger.info(
            "SQLite scheduler opened (%(pending)d pending request(s)%(recovered)s)",
            {
                "pending": pending,
                "recovered": (
                    f", {recovered} in-flight request(s) recovered from a previous run"
                    if recovered
                    else ""
                ),
            },
            extra={"spider": spider},
        )

    def _recover_inflight(self) -> int:
        with self._db:
            cur = self._db.execute(
                "UPDATE requests SET state='pending', dispatch_token=NULL, "
                "dispatched_at=NULL WHERE state='inflight'"
            )
        return cur.rowcount

    # ----------------------------------------------------------------- close

    def close(self, reason: str) -> None:
        """Commit, close the database and release the JOBDIR lock.

        In-flight requests that were never acknowledged stay in the database
        and are recovered (moved back to ``pending``) on the next
        :meth:`open`.
        """
        if not self._opened:
            return
        self._opened = False
        pending = inflight = -1
        try:
            with self._mutex:
                pending = self._count("pending")
                inflight = self._count("inflight")
                self._close_connection()
        except sqlite3.Error:
            logger.exception(
                "Error while closing the SQLite scheduler database",
                extra={"spider": self.spider},
            )
        finally:
            self._release_lock()
        if self.stats is not None:
            self.stats.set_value(
                "sqlite_scheduler/pending", pending, spider=self.spider
            )
            self.stats.set_value(
                "sqlite_scheduler/inflight", inflight, spider=self.spider
            )
        logger.info(
            "SQLite scheduler closed (%(reason)s): %(pending)d pending, "
            "%(inflight)d in-flight request(s) left for the next run",
            {"reason": reason, "pending": pending, "inflight": inflight},
            extra={"spider": self.spider},
        )

    # --------------------------------------------------------------- enqueue

    def enqueue_request(self, request: Request) -> bool:
        """Store ``request`` and its fingerprint in a single transaction.

        Returns ``False`` only for duplicates (a request with an already
        recorded fingerprint). Raises :exc:`ValueError` for requests that
        cannot be serialized, and lets :exc:`sqlite3.Error` propagate for
        storage failures; in both cases no fingerprint is recorded, so the
        request can be fixed and enqueued again.
        """
        self._check_open()
        payload = self._serialize(request)
        fingerprint = self._fingerprint(request)
        try:
            with self._mutex, self._db:
                if fingerprint is not None:
                    self._db.execute(
                        "INSERT INTO fingerprints (fingerprint, created) VALUES (?, ?)",
                        (fingerprint, time.time()),
                    )
                self._db.execute(
                    "INSERT INTO requests "
                    "(fingerprint, priority, state, payload, enqueued_at) "
                    "VALUES (?, ?, 'pending', ?, ?)",
                    (fingerprint, request.priority, payload, time.time()),
                )
        except sqlite3.IntegrityError:
            if fingerprint is None:
                # The requests table has no unique constraints, so this can
                # only be a fingerprint conflict; anything else is a bug.
                raise
            self._inc_stat("sqlite_scheduler/dupefilter_filtered")
            logger.debug(
                "Filtered duplicate request: %(request)s",
                {"request": request},
                extra={"spider": self.spider},
            )
            return False
        except sqlite3.Error:
            logger.exception(
                "Failed to enqueue request: %(request)s",
                {"request": request},
                extra={"spider": self.spider},
            )
            raise
        self._inc_stat("scheduler/enqueued")
        self._inc_stat("sqlite_scheduler/enqueued")
        return True

    # -------------------------------------------------------------- dispatch

    def next_request(self) -> Request | None:
        """Dispatch the next pending request, marking it as in-flight.

        The request is *not* deleted: it stays in-flight until acknowledged
        with :meth:`ack_request`. The returned request carries the dispatch
        token in ``request.meta[SQLiteScheduler.ACK_META_KEY]``.
        """
        self._check_open()
        with self._mutex, self._db:
            row = self._db.execute(
                "SELECT id, payload FROM requests WHERE state='pending' "
                "ORDER BY priority DESC, id ASC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            row_id, payload = row
            token = uuid.uuid4().hex
            self._db.execute(
                "UPDATE requests SET state='inflight', dispatch_token=?, "
                "dispatched_at=? WHERE id=?",
                (token, time.time(), row_id),
            )
        try:
            request = request_from_dict(
                pickle.loads(payload),  # noqa: S301
                spider=self.spider,
            )
        except Exception:
            logger.exception(
                "Failed to deserialize queued request %(row_id)d; the "
                "scheduler database may be corrupt",
                {"row_id": row_id},
                extra={"spider": self.spider},
            )
            raise
        request.meta[self.ACK_META_KEY] = (row_id, token)
        self._inc_stat("scheduler/dequeued")
        self._inc_stat("sqlite_scheduler/dequeued")
        return request

    # ------------------------------------------------------------------ ack

    def ack_request(self, request: Request) -> bool:
        """Acknowledge that the output of ``request`` has been written.

        Call this from business code (e.g. a spider callback or an item
        pipeline) *after* your own output for the request has been
        successfully written. Acknowledged requests are deleted from the
        queue and never dispatched again; their fingerprint records are kept
        and still filter out future duplicates.

        Returns ``True`` if the acknowledgement was applied. Returns
        ``False`` (a no-op) if the dispatch token does not match the current
        dispatch of the request, i.e. the acknowledgement is a duplicate or
        carries a stale token from before a restart. Raises :exc:`ValueError`
        if the request carries no dispatch token at all.
        """
        self._check_open()
        ack = request.meta.get(self.ACK_META_KEY)
        if ack is None:
            raise ValueError(
                f"Cannot acknowledge {request!r}: it carries no dispatch "
                f"token (meta key {self.ACK_META_KEY!r}). Only requests "
                "returned by SQLiteScheduler.next_request() can be "
                "acknowledged."
            )
        row_id, token = ack
        with self._mutex, self._db:
            cur = self._db.execute(
                "DELETE FROM requests WHERE id=? AND state='inflight' "
                "AND dispatch_token=?",
                (row_id, token),
            )
        if cur.rowcount == 1:
            self._inc_stat("sqlite_scheduler/acked")
            return True
        self._inc_stat("sqlite_scheduler/ack_stale")
        logger.info(
            "Ignored stale or duplicate acknowledgement for %(request)s",
            {"request": request},
            extra={"spider": self.spider},
        )
        return False

    # ----------------------------------------------------------------- misc

    def has_pending_requests(self) -> bool:
        if not self._opened:
            return False
        with self._mutex:
            row = self._db.execute(
                "SELECT 1 FROM requests WHERE state='pending' LIMIT 1"
            ).fetchone()
        return row is not None

    def __len__(self) -> int:
        """Return the number of pending (not yet dispatched) requests."""
        if not self._opened:
            return 0
        with self._mutex:
            return self._count("pending")

    def _count(self, state: str) -> int:
        row = self._db.execute(
            "SELECT COUNT(*) FROM requests WHERE state=?", (state,)
        ).fetchone()
        return row[0]

    def _check_open(self) -> None:
        if not self._opened or self._conn is None:
            raise RuntimeError(
                f"{type(self).__name__} is not open: open(spider) must be "
                "called before use, and close() must not have been called yet."
            )

    @property
    def _db(self) -> sqlite3.Connection:
        assert self._conn is not None
        return self._conn

    def _inc_stat(self, key: str) -> None:
        if self.stats is not None:
            self.stats.inc_value(key, spider=self.spider)

    def _fingerprint(self, request: Request) -> str | None:
        if request.dont_filter or not self.dedup_enabled:
            return None
        return self._fingerprinter.fingerprint(request).hex()

    def _serialize(self, request: Request) -> bytes:
        try:
            data = request.to_dict(spider=self.spider)
            meta = data.get("meta")
            if meta and self.ACK_META_KEY in meta:
                # Never persist a dispatch token from a previous scheduling
                # of the same Request object.
                data["meta"] = {
                    key: value
                    for key, value in meta.items()
                    if key != self.ACK_META_KEY
                }
            return pickle.dumps(data, protocol=4)
        except (pickle.PicklingError, AttributeError, TypeError, ValueError) as e:
            self._inc_stat("sqlite_scheduler/unserializable")
            raise ValueError(
                f"Unable to serialize request {request!r}: {e}. "
                f"{type(self).__name__} does not fall back to an in-memory "
                "queue."
            ) from e

    # ------------------------------------------------------------ locking

    def _acquire_lock(self) -> None:
        lock_path = Path(self.jobdir, self.LOCK_FILENAME)
        lock_file = lock_path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt

                lock_file.seek(0, os.SEEK_END)
                if lock_file.tell() == 0:
                    lock_file.write(b"\0")
                    lock_file.flush()
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)  # type: ignore[attr-defined]
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            lock_file.close()
            raise RuntimeError(
                f"Cannot open SQLite scheduler for JOBDIR {self.jobdir!r}: "
                "the directory is locked by another active scheduler. Only "
                "one SQLiteScheduler may use a JOBDIR at a time; after an "
                "abnormal process exit the lock is released by the OS and a "
                "new process can take over."
            ) from e
        self._lock_file = lock_file

    def _release_lock(self) -> None:
        lock_file, self._lock_file = self._lock_file, None
        if lock_file is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        except OSError:
            logger.exception(
                "Error releasing the SQLite scheduler lock",
                extra={"spider": self.spider},
            )
        finally:
            lock_file.close()

    def _close_connection(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            conn.close()
