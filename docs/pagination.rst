Paging indexed commitments
==========================

Use a SQL indexing service to page a user's distinct set CIDs or individual
object commitment receipts:

.. code-block:: python

   sets = indexing_service.find_user_collection_cids(user, limit=50, offset=0)
   objects = indexing_service.find_user_objects_page(user, limit=50, offset=0)

``sets.cids`` contains sorted lowercase CIDs from the user's ``AddSetObject``
events, not set creation receipts. ``objects.receipts`` contains one receipt
per ``AddObject`` event, including repeated commitments to the same object.
Receipts are ordered by block timestamp, chain ID, transaction hash, then
object CID. Hex identifiers are lowercase.

Pass a limit. Both pages return the matching ``total`` even when empty; use
``limit=0`` for the count alone. On unchanged data, advance the offset by the
number of items returned until you reach the total. Updates between requests
can change the count and contents. SQL checks index freshness and may scan
matching rows for the count; it returns only the requested page and count.

Web3 setup and recovery
-----------------------

Use an archival RPC endpoint that returns complete logs and consistent
finalized history across requests, including when load-balanced. An omitted
log or falsely empty response can leave commitments out of a page.

.. code-block:: python

   indexer = Web3HTTPIndexingService([commitment_service])
   indexer.sync_index(block_batch_size=1000)
   objects = indexer.find_user_objects_page(user, limit=50)

The first sync starts at genesis and ends at each source's finalized block.
``block_batch_size`` controls each log request's width, not the total history;
``n_last_blocks`` applies only to unpaged reads. Reuse the same service instance
for syncs and page requests. Pages make no RPC calls. Both views become
available after the first complete sync and remain readable during later syncs.
Use SQL if the history will not fit in memory.

After a transient failure, retry sync on the same instance; completed ranges
are retained. After a finalized-history conflict, check the provider and sync
a fresh instance before reading either view. If you change the set of indexing
sources, or restart the process, create and sync a new instance.

Failover
--------

Start each paged method at offset zero on the same failover instance. If its
selected backend becomes unavailable, retry it or discard the previous pages
and restart at zero on a new instance. Unpaged methods still fail over per call;
aggregate services do not combine independently paged results.
