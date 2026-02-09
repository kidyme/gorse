// Copyright 2025 gorse Project Authors
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

package logics

import (
	"context"
	"strings"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/gorse-io/gorse/common/expression"
	"github.com/gorse-io/gorse/common/heap"
	"github.com/gorse-io/gorse/common/util"
	"github.com/gorse-io/gorse/config"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/storage/data"
	"github.com/juju/errors"
	"github.com/samber/lo"
)

const (
	LatestRecommender          = "latest"
	NonPersonalizedRecommender = "non-personalized/"
	ItemToItemRecommender      = "item-to-item/"
	UserToUserRecommender      = "user-to-user/"
	ExternalRecommender        = "external/"
	CollaborativeRecommender   = "collaborative"
)

type Recommender struct {
	config      config.RecommendConfig
	cacheClient cache.Database
	dataClient  data.Database

	online       bool
	coldstart    bool
	userId       string
	userFeedback []data.Feedback
	categories   []string
	excludeSet   mapset.Set[string]
}

type RecommenderFunc func(ctx context.Context) ([]cache.Score, string, error)

func NewRecommender(config config.RecommendConfig, cacheClient cache.Database, dataClient data.Database, online bool, userId string, categories []string) (*Recommender, error) {
	// Load user feedback
	// 加载用户反馈记录(用户行为)
	userFeedback, err := dataClient.GetUserFeedback(context.Background(), userId, lo.ToPtr(time.Now()))
	if err != nil {
		return nil, errors.Trace(err)
	}
	excludeSet := mapset.NewSet[string]()
	coldstart := true
	for _, feedback := range userFeedback {
		// 决定是否把用户已反馈过的物品加入排除集合
		// 如果没有启用放回 或者 不是在线推荐，则把用户已反馈过的物品加入排除集合，以避免重复推荐
		if !config.Replacement.EnableReplacement || !online {
			excludeSet.Add(feedback.ItemId)
		}

		// 如果当前反馈是正反馈(PositiveFeedbackTypes)，说明用户有过正反馈，则不冷启动
		// 只要用户有过正反馈行为，就说明用户已有偏好信号，不再属于冷启动用户。这样后续推荐可以使用用户历史行为来推荐
		if expression.MatchFeedbackTypeExpressions(config.DataSource.PositiveFeedbackTypes, feedback.FeedbackType, feedback.Value) {
			coldstart = false
		}
	}
	return &Recommender{
		config:       config,
		cacheClient:  cacheClient,
		dataClient:   dataClient,
		userId:       userId,
		userFeedback: userFeedback,
		online:       online,
		coldstart:    coldstart,
		categories:   categories,
		excludeSet:   excludeSet,
	}, nil
}

func (r *Recommender) ExcludeSet() mapset.Set[string] {
	return r.excludeSet
}

func (r *Recommender) UserFeedback() []data.Feedback {
	return r.userFeedback
}

func (r *Recommender) IsColdStart() bool {
	return r.coldstart
}

func (r *Recommender) Recommend(ctx context.Context, limit int) (result []cache.Score, err error) {
	if !strings.EqualFold(r.config.Ranker.Type, "none") {
		scores, err := r.cacheClient.SearchScores(ctx, cache.Recommend, r.userId, r.categories, 0, r.config.CacheSize)
		if err != nil {
			return nil, errors.Trace(err)
		}
		result = make([]cache.Score, 0, len(scores))
		for _, score := range scores {
			if !r.excludeSet.Contains(score.Id) {
				r.excludeSet.Add(score.Id)
				result = append(result, score)
			}
		}
	} else {
		result, _, err = r.RecommendSequential(ctx, result, r.config.CacheSize, r.config.Ranker.Recommenders...)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if len(result) >= limit && limit > 0 {
		return result[:limit], nil
	}
	result, _, err = r.RecommendSequential(ctx, result, limit, r.config.Fallback.Recommenders...)
	return result, errors.Trace(err)
}

// RecommendSequential recommend items from multiple recommenders sequentially util reaching the limit.
// If limit <= 0, all recommendations are returned.
func (r *Recommender) RecommendSequential(ctx context.Context, result []cache.Score, limit int, names ...string) ([]cache.Score, string, error) {
	var digests []string
	for _, name := range names {
		// 通过推荐器名 parse 推荐器func
		recommenderFunc, err := r.parse(name)
		if err != nil {
			return nil, "", errors.Trace(err)
		}
		scores, digest, err := recommenderFunc(ctx)
		if err != nil {
			return nil, "", errors.Trace(err)
		}
		// 后续推荐器会过滤掉已推荐的
		for _, score := range scores {
			r.excludeSet.Add(score.Id)
		}
		result = append(result, scores...)
		digests = append(digests, digest)

		// 若配置了limit，且结果数量>=limit，则返回前 limit条
		if limit > 0 && len(result) >= limit {
			return result[:limit], util.MD5(digests...), nil
		}
	}
	return result, util.MD5(digests...), nil
}

func (r *Recommender) parse(fullname string) (RecommenderFunc, error) {
	if fullname == CollaborativeRecommender {
		return r.recommendCollaborative, nil
	} else if fullname == LatestRecommender {
		return r.recommendLatest, nil
	} else if strings.HasPrefix(fullname, NonPersonalizedRecommender) {
		name := strings.TrimPrefix(fullname, NonPersonalizedRecommender)
		return r.recommendNonPersonalized(name), nil
	} else if strings.HasPrefix(fullname, ItemToItemRecommender) {
		name := strings.TrimPrefix(fullname, ItemToItemRecommender)
		return r.recommendItemToItem(name), nil
	} else if strings.HasPrefix(fullname, UserToUserRecommender) {
		name := strings.TrimPrefix(fullname, UserToUserRecommender)
		return r.recommendUserToUser(name), nil
	} else if strings.HasPrefix(fullname, ExternalRecommender) {
		name := strings.TrimPrefix(fullname, ExternalRecommender)
		return r.recommendExternal(name), nil
	} else {
		return nil, errors.Errorf("unknown recommender: %s", fullname)
	}
}

// ** 推荐最新 推荐器
func (r *Recommender) recommendLatest(ctx context.Context) ([]cache.Score, string, error) {
	items, err := r.dataClient.GetLatestItems(ctx, r.config.CacheSize, r.categories)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	scores := make([]cache.Score, 0, len(items))
	for _, item := range items {
		if !r.excludeSet.Contains(item.ItemId) {
			scores = append(scores, cache.Score{
				Id:         item.ItemId,
				Score:      float64(item.Timestamp.Unix()),
				Categories: item.Categories,
			})
		}
	}
	return scores, "latest", nil
}

// ** 非个性化推荐器
// 传 name，根据name选择推荐哪个榜单/分区(subset)
func (r *Recommender) recommendNonPersonalized(name string) RecommenderFunc {
	return func(ctx context.Context) ([]cache.Score, string, error) {
		var categories []string
		if len(r.categories) == 0 {
			categories = []string{""}
		} else {
			categories = r.categories
		}
		// fetch items from cache
		items, err := r.cacheClient.SearchScores(ctx, cache.NonPersonalized, name, categories, 0, r.config.CacheSize)
		if err != nil {
			return nil, "", errors.Trace(err)
		}
		// read digest
		digest, err := r.cacheClient.Get(ctx, cache.Key(cache.NonPersonalizedDigest, name)).String()
		if err != nil {
			return nil, "", errors.Trace(err)
		}
		// remove excluded items
		return lo.Filter(items, func(item cache.Score, index int) bool {
			return !r.excludeSet.Contains(item.Id)
		}), digest, nil
	}
}

// ** CF(MF)推荐器
func (r *Recommender) recommendCollaborative(ctx context.Context) ([]cache.Score, string, error) {
	// fetch items from cache
	// 从前面CF算出来的
	items, err := r.cacheClient.SearchScores(ctx, cache.CollaborativeFiltering, r.userId, r.categories, 0, r.config.CacheSize)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	// read digest
	digest, err := r.cacheClient.Get(ctx, cache.Key(cache.CollaborativeFilteringDigest, r.userId)).String()
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	// remove excluded items
	return lo.Filter(items, func(item cache.Score, index int) bool {
		return !r.excludeSet.Contains(item.Id)
	}), digest, nil
}

// ** item2item 推荐器
func (r *Recommender) recommendItemToItem(name string) RecommenderFunc {
	return func(ctx context.Context) ([]cache.Score, string, error) {
		// filter positive feedbacks
		// 按时间顺序sort反馈
		data.SortFeedbacks(r.userFeedback)
		userFeedback := make([]data.Feedback, 0, r.config.CacheSize)
		for _, feedback := range r.userFeedback {
			// 实时推荐时，只使用前ContextSize个反馈，防止实时性不够
			if r.online && r.config.ContextSize <= len(userFeedback) {
				break
			}
			if expression.MatchFeedbackTypeExpressions(r.config.DataSource.PositiveFeedbackTypes, feedback.FeedbackType, feedback.Value) {
				// 正反馈
				userFeedback = append(userFeedback, feedback)
			}
		}
		// collect scores
		scores := make(map[string]float64)
		categories := make(map[string][]string)
		digests := mapset.NewSet[string]() // 摘要，主要用于判断是否过期或者需要重算

		// 对每个正反馈物品，获取相似物品TopK，后面再对所有这些topK的总和再取一个topK
		// 可能会有强势物品权重过大，影响其他物品的推荐效果
		// ** 考虑对正反馈的物品的贡献做归一化/衰减（例如除以其相似项数量）。
		for _, feedback := range userFeedback {
			// 该正反馈物品的相似物品 TopK
			// 具体的相似度的计算，在master侧的updateItemToItem里实现，使用HNSW，这里只是读出topK
			similarItems, err := r.cacheClient.SearchScores(ctx, cache.ItemToItem, cache.Key(name, feedback.ItemId), r.categories, 0, r.config.CacheSize)
			if err != nil {
				return nil, "", errors.Trace(err)
			}
			digest, err := r.cacheClient.Get(ctx, cache.Key(cache.ItemToItemDigest, name, feedback.ItemId)).String()
			if err != nil {
				return nil, "", errors.Trace(err)
			}
			for _, item := range similarItems {
				if !r.excludeSet.Contains(item.Id) {
					scores[item.Id] += item.Score
					categories[item.Id] = item.Categories
					digests.Add(digest)
				}
			}
		}
		// collect top scores
		filter := heap.NewTopKFilter[string, float64](r.config.CacheSize)
		// 利用heap维护topK
		for id, score := range scores {
			filter.Push(id, score)
		}

		// popAll获取topK
		elems := filter.PopAll()
		return lo.Map(elems, func(elem heap.Elem[string, float64], _ int) cache.Score {
			return cache.Score{
				Id:         elem.Value,
				Score:      elem.Weight,
				Categories: categories[elem.Value],
			}
		}), strings.Join(digests.ToSlice(), ""), nil
	}
}

// ** user2user 推荐器
func (r *Recommender) recommendUserToUser(name string) RecommenderFunc {
	return func(ctx context.Context) ([]cache.Score, string, error) {
		scores := make(map[string]float64)
		// load similar users
		// 查询TopK的相似用户
		similarUsers, err := r.cacheClient.SearchScores(ctx, cache.UserToUser, cache.Key(name, r.userId), nil, 0, r.config.CacheSize)
		if err != nil {
			return nil, "", errors.Trace(err)
		}
		// read digest
		digest, err := r.cacheClient.Get(ctx, cache.Key(cache.UserToUserDigest, name, r.userId)).String()
		if err != nil {
			return nil, "", errors.Trace(err)
		}
		// aggregate scores
		for _, user := range similarUsers {
			// load historical feedback
			// 获取这些用户的历史正反馈
			// todo 这里似乎没有限制正反馈条数，考虑限制?(条数、时间窗口)
			feedbacks, err := r.dataClient.GetUserFeedback(ctx, user.Id, lo.ToPtr(time.Now()), r.config.DataSource.PositiveFeedbackTypes...)
			if err != nil {
				return nil, "", errors.Trace(err)
			}
			// add unseen items
			for _, feedback := range feedbacks {
				if !r.excludeSet.Contains(feedback.ItemId) {
					scores[feedback.ItemId] += user.Score
				}
			}
		}
		// collect top k
		filter := heap.NewTopKFilter[string, float64](r.config.CacheSize)
		// 利用heap维护topK
		for id, score := range scores {
			filter.Push(id, score)
		}
		// popAll获取topK
		elems := filter.PopAll()
		// filter by categories
		results := make([]cache.Score, 0, len(elems))
		ids := lo.Map(elems, func(elem heap.Elem[string, float64], _ int) string {
			return elem.Value
		})
		items, err := r.dataClient.BatchGetItems(ctx, ids)
		if err != nil {
			return nil, "", errors.Trace(err)
		}
		itemsMap := make(map[string]data.Item)
		for _, item := range items {
			itemsMap[item.ItemId] = item
		}
		for _, elem := range elems {
			if item, ok := itemsMap[elem.Value]; ok && lo.Every(item.Categories, r.categories) {
				results = append(results, cache.Score{
					Id:         item.ItemId,
					Score:      elem.Weight,
					Categories: item.Categories,
				})
			}
		}
		return results, digest, nil
	}
}

// ** 外部推荐器
// todo read
func (r *Recommender) recommendExternal(name string) RecommenderFunc {
	return func(ctx context.Context) ([]cache.Score, string, error) {
		var externalConfig config.ExternalConfig
		for _, extConfig := range r.config.External {
			if extConfig.Name == name {
				externalConfig = extConfig
				break
			}
		}

		if len(r.categories) > 0 {
			// external recommenders do not support categories
			return nil, externalConfig.Hash(), nil
		}

		external, err := NewExternal(externalConfig)
		if err != nil {
			return nil, "", errors.Trace(err)
		}
		defer external.Close()
		items, err := external.Pull(r.userId)
		if err != nil {
			return nil, "", errors.Trace(err)
		}
		scores := make([]cache.Score, 0, len(items))
		for _, itemId := range items {
			if !r.excludeSet.Contains(itemId) {
				scores = append(scores, cache.Score{
					Id: itemId,
				})
			}
		}
		return scores, externalConfig.Hash(), nil
	}
}
