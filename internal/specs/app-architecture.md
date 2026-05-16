# App Architecture

`vbase-py` is organized around a small set of SDK layers.

## Commitment Services

`vbase/core/commitment_service.py` defines the `CommitmentService` abstraction
for writing commitments. Concrete implementations include direct Web3 HTTP
services, forwarder-backed services, and deterministic test variants.

## Indexing Services

`vbase/core/indexing_service.py` defines query abstractions for past
commitments. Implementations include Web3 HTTP, SQL-backed, aggregate, and
failover indexing services.

## Client

`vbase/core/vbase_client.py` exposes `VBaseClient`, the main entry point for
SDK users. It wraps commitment and indexing services behind a user-facing API.

## Data Objects

`vbase/core/vbase_object.py` defines `VBaseObject` and typed subclasses for
common payloads such as ints, floats, strings, JSON, bytes, portfolios, and
private variants.

## Datasets

`vbase/core/vbase_dataset.py` manages collections of vBase objects with
provenance. `VBaseDatasetAsync` provides non-blocking dataset operations.

## Set Matching

`vbase/core/set_matching/` provides reverse lookup services for finding sets
whose commitments match a list of object CIDs and timestamps. The chain service
combines head-based and fuzzy matching strategies.

## ABIs

Smart contract ABIs live in `vbase/core/abi/` and are packaged with the SDK.
