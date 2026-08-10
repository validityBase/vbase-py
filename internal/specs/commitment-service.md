# Commitment Service

The commitment service writes commitments either directly through Web3 or
through a forwarder. A successful transaction receipt containing a matching
contract event is authoritative for the submitted write.

## Forwarder Read-After-Write Consistency

An immediate state query after a forwarded transaction can reach a different,
lagging RPC backend. Therefore, `_add_set_worker` accepts a decoded `AddSet`
event only after confirming that its user and set CID match the submitted
request. It does not require a redundant state read after such an event.

Some successful, idempotent forwarder calls do not emit a new `AddSet` event.
In that eventless case, the service confirms `userSetCommitments` with three
bounded attempts and exponential backoff. If the commitment is still absent,
the operation raises `RuntimeError` instead of relying on an assertion.

This behavior is covered by unit tests for matching and mismatched events,
transient state-read lag, and a commitment that remains absent.

## Indexing Visibility

Transaction confirmation and indexing visibility are separate consistency
boundaries. Consumers that immediately query an indexing service after a write
may need read-your-write handling based on the returned transaction hash. That
behavior belongs at the application/indexing boundary and should be discussed
before it is added to the SDK.
