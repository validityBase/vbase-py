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
contract events. It uses event filters (`eth_newFilter` followed by
`eth_getFilterLogs`). Choose an endpoint that supports both methods across
the required block range and preserves filter state between requests.

When you use the vBase forwarder,
`IndexingService.create_instance_from_commitment_service()` connects to the
node URL returned by the forwarder unless you supply `node_rpc_url`. Its
query capabilities depend on the configured endpoint:

- Check the endpoint's supported block range. Log retrieval uses the retry policy in
  [`Web3HTTPIndexingService._retry_get_all_entries`](vbase/core/indexing_service.py);
  filter creation errors propagate directly.
- Event filters fail if a load-balanced endpoint loses filter state between
  requests, even for recent blocks.
- If the endpoint has not reached a commitment's block, that commitment
  will not appear in event queries.

To select an endpoint that meets these requirements, pass its URL:

```python
indexing_service = IndexingService.create_instance_from_commitment_service(
    vbc.commitment_service, node_rpc_url="https://<your-node-rpc-url>"
)
```

The endpoint must be on the same chain as the forwarder. The SDK does not
validate chain identity or filter support. Verify both before querying; an
empty result does not establish that you selected the correct chain.

Set `indexing_service.n_last_blocks` to control how far back from the current
block queries begin. The range calculation and full-history behavior are
defined by
[`Web3HTTPIndexingService._get_from_block`](vbase/core/indexing_service.py).
For dataset queries, see the indexing service selection documented in
[`VBaseDataset.get_commitment_receipts()`](vbase/core/vbase_dataset.py).
For large histories, you can also use a custom indexer such as
`SQLIndexingService`.

## Development

Install the generated development lock and package locally:

```bash
python -m pip install --require-hashes -r requirements/dev.txt
python -m pip install --no-deps --no-build-isolation -e .
```

See `internal/specs/python-dependency-hashes.md` in this repository for the
dependency layout, lock policy, package metadata rules, and regeneration
commands.
