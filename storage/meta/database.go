// Copyright 2024 gorse Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/gorse-io/gorse/model"
	"github.com/gorse-io/gorse/storage"
	"github.com/juju/errors"
	"github.com/samber/lo"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"golang.org/x/exp/maps"
)

const (
	COLLABORATIVE_FILTERING_MODEL = "COLLABORATIVE_FILTERING_MODEL"
	CLICK_THROUGH_RATE_MODEL      = "CLICK_THROUGH_RATE_MODEL"
	RECOMMEND_CONFIG              = "RECOMMEND_CONFIG"
)

// Model 模型元信息
type Model[T any] struct {
	ID     int64        // 模型标识
	Type   string       // 模型类型
	Params model.Params // 模型参数
	Score  T            // 模型得分，训练/搜索时的指标
}

func (m *Model[T]) ToJSON() string {
	return string(lo.Must1(json.Marshal(m)))
}

func (m *Model[T]) FromJSON(data string) error {
	return json.Unmarshal([]byte(data), m)
}

// Equal checks if two models have the same type and parameters.
func (m *Model[T]) Equal(other Model[T]) bool {
	return m.Type == other.Type && maps.Equal(m.Params, other.Params)
}

// Node 集群中的节点实例(master/server/worker)
// 用于心跳/注册与节点列表管理
type Node struct {
	UUID       string
	Hostname   string
	Type       string
	Version    string
	UpdateTime time.Time
}

// Database 抽象集群使用的元数据存储，同目录下有sqlite的实现
// sqlite实现中，有这些表:
// - nodes: 节点信息
// - cron_jobs: 定时任务
// - key_values: 键值对(模型配置、训练结果、推荐配置)
// 总之定义各种系统级元信息
type Database interface {
	// Close 释放数据库持有的资源。
	Close() error
	// Init 初始化所需的表和索引。
	Init() error
	// UpdateNode 更新或插入节点心跳与元数据。
	UpdateNode(node *Node) error
	// ListNodes 返回所有已注册的节点。
	ListNodes() ([]*Node, error)
	// Put 根据 key 存储字符串值。
	Put(key, value string) error
	// Get 根据 key 获取值；不存在时返回 nil。
	Get(key string) (*string, error)
	// Delete 删除指定 key 及其值。
	Delete(key string) error
}

// Open 打开数据库连接。
func Open(path string, ttl time.Duration) (Database, error) {
	var err error
	if strings.HasPrefix(path, storage.SQLitePrefix) {
		dataSourceName := path[len(storage.SQLitePrefix):]
		// append parameters
		if dataSourceName, err = storage.AppendURLParams(dataSourceName, []lo.Tuple2[string, string]{
			{"_pragma", "busy_timeout(10000)"},
			{"_pragma", "journal_mode(wal)"},
		}); err != nil {
			return nil, errors.Trace(err)
		}
		// connect to database
		database := new(SQLite)
		database.ttl = ttl
		if database.db, err = otelsql.Open("sqlite", dataSourceName,
			otelsql.WithAttributes(semconv.DBSystemSqlite),
			otelsql.WithSpanOptions(otelsql.SpanOptions{DisableErrSkip: true}),
		); err != nil {
			return nil, errors.Trace(err)
		}
		return database, nil
	}
	return nil, errors.Errorf("Unknown database: %s", path)
}
