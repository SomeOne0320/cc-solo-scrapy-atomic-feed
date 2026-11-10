.. _topics-scheduler:

=========
Scheduler
=========

.. module:: scrapy.core.scheduler

The scheduler component receives requests from the :ref:`engine <component-engine>`
and stores them into persistent and/or non-persistent data structures.
It also gets those requests and feeds them back to the engine when it
asks for a next request to be downloaded.


Overriding the default scheduler
================================

You can use your own custom scheduler class by supplying its full
Python path in the :setting:`SCHEDULER` setting.


Minimal scheduler interface
===========================

.. autoclass:: BaseScheduler
   :members:


Default scheduler
=================

.. autoclass:: Scheduler()
   :members:
   :special-members: __init__, __len__


.. _topics-scheduler-sqlite:

SQLite scheduler (recoverable, at-least-once)
=============================================

.. module:: scrapy.core.sqlite_scheduler

:class:`~scrapy.core.sqlite_scheduler.SQLiteScheduler` is an optional,
opt-in scheduler for long-running crawls that must survive abrupt process
termination (kill, power loss, crashes in download/parse/output code)
without losing or silently re-skipping requests.

With the default scheduler, a request is removed from the on-disk queue as
soon as it is dispatched, while its fingerprint is recorded separately. If
the process dies while the request is being downloaded, parsed or written
to your storage, the request is gone from the queue but its fingerprint may
already be recorded, so it is neither retried nor re-enqueueable.

The SQLite scheduler instead keeps the request queue and the duplicate
filter in a **single SQLite database** inside :setting:`JOBDIR`, and only
drops a request from the queue when your code explicitly confirms that its
output has been written.

Enabling it
-----------

.. code-block:: python

    SCHEDULER = "scrapy.core.sqlite_scheduler.SQLiteScheduler"
    JOBDIR = "crawls/somespider-1"  # mandatory

:setting:`JOBDIR` is **required**: without it, initialization fails with a
:exc:`ValueError`. The scheduler never falls back to in-memory queues, and
requests that cannot be serialized are rejected with a :exc:`ValueError`
instead of being silently rerouted to a memory queue.

How it works
------------

All state lives in ``<JOBDIR>/scheduler.sqlite``:

* Enqueuing a request records its :ref:`fingerprint <request-fingerprints>`
  and its serialized payload in the **same SQLite transaction**. A
  committed enqueue is therefore fully recoverable after a crash, and a
  failed enqueue (serialization error or transaction rollback) never leaves
  a fingerprint behind that would block a retry.

* Requests are dispatched by descending :attr:`~scrapy.Request.priority`;
  requests with the same priority are dispatched in FIFO (enqueue) order.

* :meth:`~scrapy.core.sqlite_scheduler.SQLiteScheduler.next_request` marks
  the dispatched request as *in-flight* instead of deleting it, and
  attaches a dispatch token to
  ``request.meta[SQLiteScheduler.ACK_META_KEY]``.

* :meth:`~scrapy.core.sqlite_scheduler.SQLiteScheduler.ack_request`
  deletes the in-flight request, but only if the token matches the current
  dispatch. Acknowledgements are idempotent: acknowledging twice is a
  no-op, and a stale token from a previous run cannot acknowledge a
  re-dispatched request.

* When the scheduler opens, any request left in-flight by a previous run is
  moved back to *pending* and will be dispatched again. Acknowledged
  requests are never dispatched again, but their fingerprints are kept and
  still filter out future duplicates.

* Requests with :attr:`~scrapy.Request.dont_filter` set to ``True`` are
  never deduplicated; each one gets its own persistent record.

Acknowledging requests from business code
-----------------------------------------

HTTP success, a callback returning, or items passing through item pipelines
do **not** acknowledge a request. Only an explicit call to
:meth:`~scrapy.core.sqlite_scheduler.SQLiteScheduler.ack_request` does,
after *your* output has been successfully written:

.. code-block:: python

    class MySpider(scrapy.Spider):
        name = "myspider"
        start_urls = ["https://example.com/catalog/1"]

        def parse(self, response):
            rows = list(self.extract_rows(response))
            # 1. Write your business output first, idempotently.
            self.db.upsert_many(
                rows,
                # 2. Use a stable business key so a redelivery of the same
                #    page does not create duplicates (see below).
                idempotency_key=f"catalog:{response.meta['page']}",
            )
            # 3. Only then acknowledge the request.
            self.crawler.engine.scheduler.ack_request(response.request)
            for url in response.css("a.next::attr(href)").getall():
                yield response.follow(url, callback=self.parse)

``ack_request()`` returns ``True`` if the acknowledgement was applied and
``False`` if it was a no-op (duplicate or stale token). It raises
:exc:`ValueError` if the request carries no dispatch token, e.g. because it
was never dispatched by this scheduler.

.. note:: The acknowledgement is intentionally decoupled from item
   pipelines: whether an item pipeline is the right place to call
   ``ack_request()`` depends on where your output transaction commits. Call
   it wherever you can be sure the output of *that request* is durably
   stored.

Delivery semantics and idempotency
----------------------------------

Delivery is **at-least-once**. Your business output and the local
acknowledgement are not the same transaction, so a crash after writing
output but before acknowledging causes the request to be delivered again on
the next run. Make your outputs idempotent to tolerate that:

* Key your output rows by a stable **business idempotency key** (e.g. the
  canonical URL, a product ID, or ``<spider>:<url>:<content-hash>``) and
  use upserts (``INSERT ... ON CONFLICT DO UPDATE``, ``REPLACE INTO``,
  ``$setOnInsert``, …) instead of blind inserts.

* Alternatively, record processed idempotency keys in your output storage
  and skip duplicates there.

* The dispatch token in ``request.meta[SQLiteScheduler.ACK_META_KEY]`` is
  *not* an idempotency key: it changes on every dispatch of the same
  request.

Recovery boundaries
-------------------

* A **committed enqueue** survives any crash.
* A **failed enqueue** (unserializable request, storage error, transaction
  rollback) leaves no fingerprint: fix the cause and enqueue again.
* A request **dispatched but not acknowledged** (crash anywhere in
  download, parsing or output writing, or a spider closed before
  acknowledging) is dispatched again on the next run.
* An **acknowledged** request is never dispatched again; its fingerprint
  still filters future duplicates of it.
* A **duplicate** request is dropped at enqueue time and never reaches the
  queue.

Concurrency and locking
-----------------------

A :setting:`JOBDIR` may only be used by **one active scheduler at a time**.
While open, the scheduler holds an exclusive OS-level lock on
``<JOBDIR>/scheduler.sqlite.lock``; a second process trying to open a
scheduler on the same directory fails with a :exc:`RuntimeError`. If the
process dies, the OS releases the lock and a new process can take over the
same directory.

Settings compatibility
----------------------

* :setting:`DUPEFILTER_CLASS`: deduplication is built into this scheduler
  so that fingerprints share the enqueue transaction. The default
  ``scrapy.dupefilters.RFPDupeFilter`` value enables it;
  ``scrapy.dupefilters.BaseDupeFilter`` disables it. Any other value raises
  :exc:`ValueError` at initialization.
* :setting:`REQUEST_FINGERPRINTER_CLASS` is honored for fingerprint
  computation.
* :setting:`SCHEDULER_PRIORITY_QUEUE`, :setting:`SCHEDULER_DISK_QUEUE`,
  :setting:`SCHEDULER_MEMORY_QUEUE`, :setting:`SCHEDULER_START_DISK_QUEUE`
  and :setting:`SCHEDULER_START_MEMORY_QUEUE` are not supported (storage is
  SQLite, not queuelib); non-default values are ignored with a logged
  warning.
* :setting:`SCHEDULER_DEBUG` has no effect: unserializable requests are
  always reported through the ``sqlite_scheduler/unserializable`` stat and
  a raised :exc:`ValueError`.

Stats
-----

In addition to the standard ``scheduler/enqueued`` and
``scheduler/dequeued`` stats, the SQLite scheduler reports:

========================================= ===========================================
Stat                                      Meaning
========================================= ===========================================
``sqlite_scheduler/enqueued``             Requests stored in the SQLite queue
``sqlite_scheduler/dequeued``             Requests dispatched (marked in-flight)
``sqlite_scheduler/acked``                Successful acknowledgements
``sqlite_scheduler/ack_stale``            Ignored duplicate/stale acknowledgements
``sqlite_scheduler/dupefilter_filtered``  Requests dropped as duplicates
``sqlite_scheduler/unserializable``       Requests rejected as unserializable
``sqlite_scheduler/recovered``            In-flight requests recovered at open
``sqlite_scheduler/pending``              Pending requests at close
``sqlite_scheduler/inflight``             Unacknowledged requests at close
========================================= ===========================================

.. autoclass:: SQLiteScheduler()
   :members:
   :special-members: __len__

