# SeiDB Layout

This directory contains the in-tree SeiDB adaptation for `cosmos-sdk-inj`.

## Structure

- `config/`: shared SeiDB configuration types.
- `common/`: cross-cutting helpers shared by SC/SS layers.
- `commitment/`: SDK-facing commitment store wrapper.
- `sc/`: State Commitment layer, aligned with SeiDB SC concepts.
- `ss/`: State Store layer, aligned with SeiDB SS concepts.
  - `state/`: read-only historical KV adapter used by query/cache paths.
  - `types/`: SS shared interfaces and payload types.
  - `utils/`: SS shared conversion and cloning helpers.
  - `cosmos/`, `evm/`, `composite/`, `pruning/`: SS subcomponents split to mirror SeiDB.
- `db_engine/pebbledb/mvcc/`: Pebble MVCC engine used by SS.
- `rootmulti/`: Cosmos SDK orchestration layer. It wires SC/SS into multistore flow, but should not own SS internals.
- `docs/`: local notes and comparisons.

## Migration Logic

This tree is being reshaped to follow upstream `sei-db` as closely as possible while preserving existing behavior.

The migration rule is:

1. Keep Cosmos SDK orchestration in `rootmulti/`.
2. Move storage-specific logic out of `rootmulti/` into `sc/`, `ss/`, and `db_engine/`.
3. Extract shared config/types/utils first, then relocate implementations, to avoid package cycles.
4. Prefer structural alignment with `sei-db` over local shortcuts, unless the SDK integration requires a thin adapter.

In short: `rootmulti` coordinates, `sc` commits, `ss` serves historical state, and `db_engine` stores MVCC data.
