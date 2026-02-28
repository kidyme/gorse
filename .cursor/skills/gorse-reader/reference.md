# Gorse Reference — Detailed Architecture

Read this when you need deeper context on boot flow, recommender pipelines, or design decisions.

## Master Boot (master.serve)

Order of operations in `master/master.go`:

1. **Blob**: Create local `blobServer` dir, configure `blobStore` (local/cloud/proxy).
2. **Meta DB**: Connect to `meta.sqlite3` for config and model metadata persistence.
3. **Data & cache**: Init `DataClient` / `CacheClient`, run `Init()` for tables.
4. **Restore state**: Load `Recommend` config, CF/CTR model metadata.
5. **Background tasks**: `RunTasksLoop()` — periodic dataset load and training.
6. **gRPC**: Register master/data/cache/blob gRPC handlers (TLS supported).
7. **OIDC** (optional): Auth provider, OAuth2, token cache.
8. **HTTP**: REST API + Dashboard via `StartHttpServer()`.

## Recommender Pipelines

### MF (Collaborative Filtering)

- **Master**: Trains user/item vectors.
- **Worker**: Uses user vector to query item vector index (ANN), builds per-user CF cache (`CollaborativeFiltering`).
- **Online**: Server reads CF cache only.

### Item-to-Item

- **Master**: Builds per-item similar TopK by type (embedding/tags/users/auto), caches (`ItemToItem`).
- **Online**: For each user positive feedback item → read similar TopK → aggregate scores → global TopK.

### User-to-User

- **Master**: Builds per-user similar TopK (embedding/tags/items/auto), caches (`UserToUser`).
- **Online**: Similar users → their positive feedback items → aggregate → TopK.

### Why CF Cache Is Computed on Worker

- Master: trains model, produces vectors.
- Worker: uses vectors to build user-level recommendation cache.
- User-level cache is batch-style and parallelizable (`parallel.Detachable`).
- Keeps master load low; worker is closer to cache write path.
- Supports per-user recompute without master involvement.

## Storage Layers

| Layer | Purpose | Backends |
|-------|---------|----------|
| `storage/data` | Users, items, feedback | MySQL, MongoDB, Postgres, ClickHouse |
| `storage/cache` | Recommendation caches | Redis, MySQL, MongoDB, Postgres |
| `storage/meta` | Config, model metadata | SQLite |
| `storage/blob` | Model files, large objects | Local, S3, GCS, Azure |

## Protocol

gRPC definitions in `protocol/`. Master, Worker, and Server communicate via generated stubs (`*.pb.go`).
