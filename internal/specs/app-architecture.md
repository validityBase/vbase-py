# App Architecture

`vbase-py` is organized around a small set of SDK layers.

## Commitment Services

`vbase/core/commitment_service.py` defines the `CommitmentService` abstraction
for writing commitments. Concrete implementations include direct Web3 HTTP
services, forwarder-backed services, and deterministic test variants.

`ForwarderCommitmentService` depends on the response format of the vBase
forwarder:

- Failures use HTTP error statuses with `application/problem+json` bodies,
  which the SDK raises as `ProblemDetailsError`.
- Successful responses have the form `{"success": true, "data": ..., "log": ...}`.
  The SDK treats only `"success": false`, which older forwarders returned,
  as a failure in a 2xx response.
- `commitment-service-data` returns `node_rpc_url` and
  `commitment_service_address`. The URL is the forwarder's keyless public
  node; the forwarder does not return its private node URL.
  The SDK reads only these fields and ignores
  any others. The response has no middleware options, such as
  `inject_geth_poa_middleware`, so the forwarder's chain must not need them.
  Serving a proof-of-authority chain requires adding the option to the
  response and to the SDK together.

## Indexing Services

`vbase/core/indexing_service.py` defines query abstractions for past
commitments. Implementations include Web3 HTTP, SQL-backed, aggregate, and
failover indexing services.

See [Indexing and Node Requirements](../../README.md#indexing-and-node-requirements)
for endpoint selection and event-query requirements, and
[Forwarder Indexing Configuration](development.md#forwarder-indexing-configuration)
for the integration tests' explicit RPC selection.

## Client

`vbase/core/vbase_client.py` exposes `VBaseClient`, the main entry point for
SDK users. It wraps commitment and indexing services behind a user-facing API.

## Data Objects

`vbase/core/vbase_object.py` defines `VBaseObject` and typed subclasses for
common payloads such as ints, floats, strings, JSON, bytes, portfolios, and
private variants.

## Datasets

`vbase/core/vbase_dataset.py` manages collections of vBase objects with
provenance. `vbase/core/vbase_dataset_async.py` provides `VBaseDatasetAsync`
for non-blocking dataset operations.

## Set Matching

`vbase/core/set_matching/` provides reverse lookup services for finding sets
whose commitments match a list of object CIDs and timestamps. The chain service
combines head-based and fuzzy matching strategies.

## ABIs

Smart contract ABIs live in `vbase/core/abi/` and are packaged with the SDK.
