---
name: gorse-reader
description: Guide for reading and navigating the gorse recommender system codebase. Use when the user is reading, exploring, or asking about gorse, the recommendation system architecture, module structure, data flow, or how components interact. Supports both Chinese and English queries.
---

# Gorse Reader

Quick reference for navigating the gorse (recommender system) codebase. Gorse is an AI-powered open-source recommender written in Go with single-node training and distributed prediction.

## Architecture Overview

Gorse has three node types:

| Node | Role | Key Code Path |
|------|------|---------------|
| **Master** | Model training, task scheduling, config, dashboard | `master/`, `cmd/gorse-master/` |
| **Server** | REST APIs, online real-time recommendations | `server/`, `cmd/gorse-server/` |
| **Worker** | Offline recommendations per user | `worker/`, `cmd/gorse-worker/` |

All-in-one mode: `cmd/gorse-in-one/` runs master + server + worker together (e.g. playground).

## Directory Map

When answering "where is X?" or "how does Y work?", use this map:

```
cmd/              Binary entry points: gorse-master, gorse-server, gorse-worker, gorse-in-one
master/           Training, RunTasksLoop, config restore, gRPC/REST, dashboard
server/           Online recommend API, REST endpoints (server/rest.go)
worker/           Offline recommendation pipeline, periodic tasks
logics/           Recommendation strategies: item-to-item, user-to-user, CF, non-personalized, external
model/            Models: model/cf/ (collaborative filtering), model/ctr/ (AFM for ranking)
dataset/          Dataset structure, indexes, splits for training
storage/          Data layer: storage/data (users/items/feedback), storage/cache (recommendation caches),
                 storage/meta (config/model metadata), storage/blob (model files, cloud storage)
protocol/         gRPC protobuf definitions (Master/Worker/Server communication)
config/           Config structs and defaults
common/           Utilities: ANN, BLAS, floats, heap, parallel, expression, log
client/           Client config, examples
notes/            Project notes (note.md) — useful Chinese summaries
```

## Boot & Data Flow

**Master startup** (`master/master.go` → `serve()`):

1. Init blob store (local/cloud)
2. Connect meta DB (config, model metadata)
3. Connect data + cache DB, `Init()` schema
4. Restore state from meta
5. `RunTasksLoop()`: periodic dataset load + training
6. Start gRPC + HTTP (REST, dashboard)

**Recommendation flow**:

- **CF (collaborative)**: Master trains user/item vectors → Worker builds user-level CF cache from vectors → Server reads cache for `/api/recommend`
- **Item-to-item**: Master computes per-item similar TopK, caches; online: expand user's positive feedback items → similar items → aggregate scores
- **User-to-user**: Master computes per-user similar TopK; online: similar users → their feedback items → TopK
- **Non-personalized**: Master builds (e.g. popular/latest) lists → cached; Server serves from cache

## Key Files to Read First

| Goal | File |
|------|------|
| Master boot & tasks | `master/master.go`, `master/tasks.go` |
| REST API | `server/rest.go`, `server/server.go` |
| Recommendation logic | `logics/recommend.go`, `logics/item_to_item.go`, `logics/user_to_user.go` |
| CF model | `model/cf/model.go` |
| CTR model | `model/ctr/model.go` |
| Storage interface | `storage/data/database.go`, `storage/cache/database.go` |
| Worker pipeline | `worker/worker.go`, `worker/pipeline.go` |
| Config | `config/config.go` |
| Chinese notes | `notes/note.md` |

## Recommender Types (logics/recommend.go)

```
LatestRecommender          = "latest"
NonPersonalizedRecommender = "non-personalized/"
ItemToItemRecommender      = "item-to-item/"
UserToUserRecommender      = "user-to-user/"
ExternalRecommender        = "external/"
CollaborativeRecommender   = "collaborative"
```

## Model Roles

- **CF (model/cf/)**: Matrix factorization for recall (user/item embeddings).
- **CTR (model/ctr/)**: AFM model for ranking (click-through prediction). Used when `ranker.type != "none"`.

## When User Asks in Chinese

Support Chinese questions about gorse. Key terms:

- 召回 → recall, CF
- 排序 → ranking, CTR
- 协同过滤 → collaborative filtering (CF)
- 冷启动 → cold start (`IsColdStart()` in `logics/recommend.go`)
- 反馈 / 正反馈 → feedback, positive feedback
- 非个性化推荐 → non-personalized recommendation

## Search Tips

- Training loop: search for `RunTasksLoop`, `loadDataset`, `Fit` in `master/tasks.go`
- Recommendation request path: `server/rest.go` → `Recommender` → `logics/recommend.go`
- Cache keys: `storage/cache/` (Redis, SQL, MongoDB backends)

## Additional Resources

- For more detailed architecture and boot flow, see [reference.md](reference.md)
- Official docs: https://gorse.io/docs/
