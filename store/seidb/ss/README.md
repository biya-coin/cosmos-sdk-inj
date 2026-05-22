This directory contains the SeiDB State Store (SS) layer.

Layout mirrors the SC split:

- `store.go`: SS entrypoint
- `state/`: read-only historical KV adapter used by rootmulti query/cache paths
- `impl/`: SS implementations (composite, cosmos, evm, pebble mvcc, pruning, modes)

`rootmulti/` should keep orchestration only.
