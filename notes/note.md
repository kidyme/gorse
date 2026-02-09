# NOTE

## 运行入口（各节点进程）

cmd/：二进制入口。gorse-master、gorse-server、gorse-worker、gorse-in-one 都在这里。
Master 节点逻辑
master/：训练、任务调度、配置管理、集群/成员管理、管理面板与 REST（Master 侧）。
Server 节点逻辑
server/：在线推荐与 REST API（对外服务）。
Worker 节点逻辑
worker/：离线推荐管线、周期任务。
推荐策略与编排
logics/：推荐策略实现与组合（item-to-item、user-to-user、CF、非个性化、外部策略等）。
模型与训练
model/：协同过滤（model/cf/）、CTR（model/ctr/）、评估与优化。
数据与特征结构
dataset/：训练数据集结构、索引与切分。
存储层
storage/：统一数据层接口与实现。
storage/data/：业务数据（用户、物品、反馈）。
storage/cache/：推荐结果缓存。
storage/meta/：元数据。
storage/blob/：模型/大文件存储。
协议与通信
protocol/：gRPC 协议定义与生成代码（Master/Worker/Server 之间通信）。
配置
config/：配置结构与默认配置模板。
通用组件
common/：工具与基础算法（ANN、BLAS、NN、并行、日志等）。
客户端与示例
client/：客户端配置、示例、测试脚本。
测试与 CI
各目录下 _test.go + .github/workflows/。

## BOOT

### master.serve()

作用：启动 master 节点，完成存储连接、状态恢复、后台任务与对外服务的整体初始化。

流程要点（按执行顺序）：

- 初始化 blob：创建本地 `blobServer` 目录服务并按配置创建 `blobStore`（本地/云/代理）。
- 连接 meta 数据库：`meta.sqlite3`，用于推荐配置与模型元数据的持久化。
- 连接业务数据与缓存数据库：`DataClient`/`CacheClient`，并 `Init()` 表结构。
- 从 meta 恢复状态：加载 `Recommend` 配置、协同过滤/CTR 模型元信息(只有CF和CTR需要持久化模型参数)。
- 启动后台任务：`RunTasksLoop()` 定时触发数据集加载与训练调度。
- 启动 gRPC 服务：注册 master/data/cache/blob 的 gRPC 接口，支持 TLS。
- 可选启用 OIDC：初始化 provider/verifier、OAuth2 配置与 token 缓存。
- 启动 HTTP 服务：REST API 与 Dashboard（`StartHttpServer()`）。

代码路径：`master/master.go`

## 模型

### CF、协同过滤

用于召回

### CTR

CTR 本身不是一种模型，而是一个评估指标，全称为 Click-Through Rate(点击率)

CTR 模型通常是指用于预测用户点击概率的机器学习模型，即 CTR 预估模型(CTR Prediction Model)

用于排序，项目里的CTR用的是AFM模型

## 推荐器流

MF（协同过滤）：
master 侧训练得到 user/item 向量；worker 侧用 用户向量在物品向量索引中检索 TopK，生成 CF 推荐缓存（CollaborativeFiltering）。推荐器使用时只是读取这个缓存。
item2item：
master 侧按配置类型（embedding/tags/users/auto）离线计算每个物品的相似 TopK 并缓存（ItemToItem）。在线推荐时，对用户的每个正反馈物品 读取其相似 TopK 并累加分数，再取全局 TopK。
user2user：
master 侧按配置类型（embedding/tags/items/auto）离线计算每个用户的相似 TopK 并缓存（UserToUser）。在线推荐时，先取相似用户，再汇总这些用户的正反馈物品并取 TopK。
这样既区分了“离线计算/在线读取”，也说明了不同类型不一定是向量相似。

### 为什么CF推荐缓存放在worker侧计算

> todo 后面细理为什么
这里放在 worker 侧主要是工程与性能上的权衡：
master 侧负责训练模型（产出 user/item 向量），而 worker 侧负责用向量生成用户级推荐缓存，职责分离。
推荐缓存是按用户批量生成，更适合 worker 并行做（parallel.Detachable），扩展性更好。
避免 master 负载过高：master 还要做训练、任务调度、全局计算，挪到 worker 能分散压力。
worker 更贴近推荐缓存写入路径，写 cache 更直接。
这样也方便按需重算（比如某些用户需要重算时，worker 可以局部跑）。
所以：master 产向量，worker 用向量生成 CF 推荐缓存，这是刻意的架构分工。
