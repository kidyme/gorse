# Master API 汇总

## RESTful API（`master/rest.go`）

### Dashboard / 监控

- `GET /api/dashboard/userinfo` 获取用户信息
- `GET /api/dashboard/cluster` 集群节点信息
- `GET /api/dashboard/stats` 统计信息
- `GET /api/dashboard/tasks` 任务进度
- `GET /api/dashboard/timeseries/{name}` 时间序列数据

### 配置

- `GET /api/dashboard/config` 获取配置
- `POST /api/dashboard/config` 更新配置
- `DELETE /api/dashboard/config` 删除配置
- `GET /api/dashboard/config/schema` 配置 schema

### 推荐与相似

- `GET /api/dashboard/recommend/{user-id}` 用户推荐
- `GET /api/dashboard/recommend/{user-id}/{recommender}` 指定推荐器
- `GET /api/dashboard/recommend/{user-id}/{recommender}/{name}` 指定命名推荐器
- `GET /api/dashboard/item-to-item/{name}/{item-id}` 物品相似
- `GET /api/dashboard/user-to-user/{name}/{user-id}` 用户相似
- `GET /api/dashboard/non-personalized/{name}` 非个性化推荐
- `GET /api/dashboard/latest` 最新物品

### 用户/反馈

- `GET /api/dashboard/users` 用户列表
- `GET /api/dashboard/user/{user-id}` 用户详情
- `GET /api/dashboard/user/{user-id}/feedback/` 用户反馈

### 批量导入导出

- `GET /api/bulk/users` 导出用户
- `POST /api/bulk/users` 导入用户
- `GET /api/bulk/items` 导出物品
- `POST /api/bulk/items` 导入物品
- `GET /api/bulk/feedback` 导出反馈
- `POST /api/bulk/feedback` 导入反馈

## 运维

- `POST /api/purge` 清理数据
- `GET /api/dump` 导出数据
- `POST /api/restore` 恢复数据
- `POST /api/chat` Chat 接口

## gRPC（`master/rpc.go` + `master/master.go` 注册）

### Master 服务

- `GetMeta`：节点拉取配置与元信息
- `PushProgress`：节点上报任务进度

### 存储服务

- `CacheStore`（缓存服务）
- `DataStore`（数据存储服务）
- `BlobStore`（对象存储服务）
