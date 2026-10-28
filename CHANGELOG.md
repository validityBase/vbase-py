# Changelog

All notable changes are documented here, following
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Earlier releases were not tracked in this file; see the
[commit history](https://github.com/validityBase/vbase-py/commits/main/)
for prior changes.

## [Unreleased]

### Changed

- Require Python 3.11 or later. Drop support for Python 3.8 through 3.10 (#84).
- Accept Web3 versions greater than or equal to 7.15 and less than 9 (#106).
- Set upper bounds for runtime dependencies (#63, #106).
- Remove the unused `imageio` runtime dependency (#110).

### Fixed

- Use a matching `AddSet` receipt event to confirm a new set commitment.
  Check the event's user and set CID. This avoids a false `add_set` failure
  when an RPC backend returns stale state (#86).
- Retry the state check if a successful `add_set` transaction has no `AddSet`
  receipt event. Fail if the set commitment cannot be confirmed after the
  configured retries. Commitment confirmation does not guarantee immediate
  visibility through an indexing service (#86).
