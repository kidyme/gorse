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