# Changelog

All notable changes are documented here, following
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Earlier releases were not tracked in this file; see the
[commit history](https://github.com/validityBase/vbase-py/commits/main/)
for prior changes.

## [Unreleased]

### Changed

- Require Python 3.11 or newer; Python 3.8 through 3.10 are no longer supported (#84).
- Allow Web3 versions from 7.15 up to, but excluding, 9 and constrain runtime
  dependencies to supported version ranges (#63, #106).
- Remove the unused `imageio` runtime dependency (#110).

### Fixed

- Avoid false `add_set` failures when a successful commitment is not yet visible
  through another RPC backend. Validate the receipt's user and set CID, and retry
  state confirmation for eventless, idempotent transactions (#86).
