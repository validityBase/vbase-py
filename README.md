# vbase-py

vBase Python Software Development Kit (SDK)

- Python 3.11+ support

---

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE.txt](https://github.com/validityBase/vbase-py/blob/main/LICENSE.txt) file for details.

## Introduction

vBase creates a global auditable record of when data was created, by whom, and how it has changed (collectively, “data provenance”). Data producers can prove the provenance of their data to any external party, increasing its value and marketability. Data consumers can ensure the integrity of historical data and any derivative calculations. The result is trustworthy information that can be put into production quickly without expensive and time-consuming trials.

Verifiable provenance establishes the credibility of data and calculations. For example, if you wish to prove investment skill, the recipient must be sure they are receiving a complete and accurate record of your timestamped trades or portfolios.

vBase resolves several expensive market failures common to financial data. Some of the areas that benefit include:
- Provably point-in-time datasets
- Auditable investing track records
- Sound backtests, historical simulations, and time-series modeling

vBase services do not require access to the data itself, assuring privacy. They also do not rely on centralized intermediaries, eliminating the technical, operating, and business risks of a trusted party controlling your data and its validation. vBase ensures data security and interoperability that is unattainable with legacy centralized systems. It does so by storing digital fingerprints of data, metadata, and revisions on secure public blockchains.

With vBase, creating and consuming provably correct data is as easy as pressing a button.

![Demo](https://github.com/validityBase/vbase-py/assets/153264511/4c658cdf-7287-4909-b2b2-94f2079d2d5a)

## Installation

Install the package from PyPI:

```bash
pip install vbase
```

See the [local installation](https://docs.vbase.com/python-sdk/python-quickstart-readme/quickstart-1) and the [cloud notebooks](https://docs.vbase.com/python-sdk/python-quickstart-readme/quickstart) guides to get started using vBase from Python.

## Indexing and Node Requirements

`Web3HTTPIndexingService` finds past commitments by querying a node for
contract events with `eth_getLogs`. Each query runs from a start block to the
latest block. Queries that fail with a connection error or a JSON-RPC error
from the node are retried with exponential backoff, and the last error is
raised if all attempts fail. Other errors, such as invalid query arguments, are
raised without a retry.

When you use the vBase forwarder,
`IndexingService.create_instance_from_commitment_service()` connects to the
node URL returned by the forwarder unless you supply `node_rpc_url`. The
forwarder returns a keyless public node URL and does not return its private
node URL. Supply your own archival RPC endpoint when the public node cannot
support indexing. Results depend on the endpoint:

- Many public endpoints limit the block range or result count of an
  `eth_getLogs` request. Full-history queries can fail on these endpoints.
  Limit the range with `n_last_blocks`, or use a node without these limits.
- If the endpoint has not yet reached a commitment's block, query results do
  not include that commitment.

To use a different endpoint, pass its URL:

```python
indexing_service = IndexingService.create_instance_from_commitment_service(
    vbc.commitment_service, node_rpc_url="https://<your-node-rpc-url>"
)
```

You are responsible for selecting an endpoint on the forwarder's chain. The
SDK does not check the endpoint's chain. A node on a different chain returns
empty results instead of an error, so an empty result does not confirm that
the endpoint is correct.

Set `indexing_service.n_last_blocks` to query only recent blocks. When it is
`None`, the default, queries start at the genesis block. When it is an
integer, queries start that many blocks before the current block, or at the
genesis block if the chain is shorter.

`VBaseDataset.get_commitment_receipts()` and
`VBaseDataset.try_restore_timestamps_from_index()` use the dataset's
`indexing_service` attribute if it is set. Otherwise, they create one from the
dataset's commitment service as described above. To customize queries, assign
a configured indexing service to that attribute first.

For large histories, use an indexer that does not query the node directly,
such as `SQLIndexingService`.

Published packages that use `eth_newFilter` and `eth_getFilterLogs` also need
an endpoint that supports those methods and preserves filter state between
requests. A provider's "archive" label alone does not guarantee suitable
block-range or result limits.

## Development

Install the generated development lock and package locally:

```bash
python -m pip install --require-hashes -r requirements/dev.txt
python -m pip install --no-deps --no-build-isolation -e .
```

See `internal/specs/python-dependency-hashes.md` in this repository for the
dependency layout, lock policy, package metadata rules, and regeneration
commands.

### Forwarder integration tests

The tests use `VBASE_TEST_INDEXING_NODE_RPC_URL` for indexing and obtain the
contract address from the forwarder. This is test configuration; setting it
does not change SDK defaults in an application. The test helper supports
published packages without the factory's `node_rpc_url` argument.

For a forwarder and commitment-service node on this machine:

```bash
export VBASE_API_KEY="<key accepted by the localhost forwarder>"
bash vbase/tests/scripts/run_tests_forwarder_localhost.sh
```

The localhost config expects the forwarder on port 8081 and the node on port
8545, so port 3000 can remain occupied by another service. If the forwarder
listens on another port, set `VBASE_FORWARDER_PORT` to that port. If it uses a
remote chain, set `VBASE_TEST_INDEXING_NODE_RPC_URL` to an archival endpoint
on that chain.

For the public dev service:

```bash
export VBASE_API_KEY="<public dev API key>"
export VBASE_TEST_INDEXING_NODE_RPC_URL="<archival RPC URL on the dev chain>"
bash .github/scripts/run_tests_forwarder_pub_dev.sh
```

Both public-dev GitHub workflows require secrets with these names. The
runner generates its own signer, so no signer-private-key secret is needed.
The PyPI workflow runs checkout tests against the installed package. See the
[development guide](https://github.com/validityBase/vbase-py/blob/main/internal/specs/development.md#forwarder-indexing-configuration)
for node requirements, secret setup, and source/PyPI test behavior.
