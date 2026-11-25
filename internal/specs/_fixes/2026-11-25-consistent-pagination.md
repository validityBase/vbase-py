---
slug: indexing-pagination
date: 2026-11-25
triage: no-spec
acs: []
commit: 8180dda7baf0055fed8d4b1c8d7b8621f1b3ce77
recurrence_of: none
---

# Fix: Keep paged reads on one backend and reject conflicting Web3 history

## Symptom

[PR #164 review](https://github.com/validityBase/vbase-py/pull/164#pullrequestreview-6058679270)
identified two ways to return misleading pages: failover could change sources
between offsets, and a Web3 sync could publish events from conflicting histories.

## Root cause

Failover selected the first available backend for each call, including later
pages. Web3 sampled finalized history without checking that each new range
joined its checkpoint or that returned logs agreed with block headers.

## The pinning test

`test_paged_reads_do_not_switch_on_failure_or_recovery` exercises both
directions of a backend switch. `test_history_switch_between_ranges_is_not_published`
exercises a provider changing histories during sync. Before the fix, the
expected rejections failed with `AssertionError: RuntimeError not raised`.
A sparse 10,000-block regression also checks that ten batches cost 20 log
queries and 40 header reads; reading the resulting pages makes no RPC calls.

## Spec patch

There is no feature-specific acceptance-criteria file. The README and
`internal/specs/app-architecture.md` now describe per-method backend selection,
when to restart a session, and how Web3 sync treats conflicts and outages.

## Follow-ups

Range queries require a trusted archival provider: boundary and returned-log
checks cannot detect omitted logs or a falsely empty response. Per-block
verification and app-level storage are outside this change.
