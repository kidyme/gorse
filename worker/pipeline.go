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

package worker

import (
	"context"
	"strings"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/gorse-io/gorse/common/expression"
	"github.com/gorse-io/gorse/common/log"
	"github.com/gorse-io/gorse/common/monitor"
	"github.com/gorse-io/gorse/common/parallel"
	"github.com/gorse-io/gorse/common/util"
	"github.com/gorse-io/gorse/config"
	"github.com/gorse-io/gorse/logics"
	"github.com/gorse-io/gorse/model/ctr"
	"github.com/gorse-io/gorse/storage/cache"
	"github.com/gorse-io/gorse/storage/data"
	"github.com/juju/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type Pipeline struct {
	Config                   *config.Config
	CacheClient              cache.Database
	DataClient               data.Database
	Tracer                   *monitor.Monitor
	Jobs                     int
	MatrixFactorizationItems *logics.MatrixFactorizationItems
	MatrixFactorizationUsers *logics.MatrixFactorizationUsers
	ClickThroughRateModel    ctr.FactorizationMachines
	dontskipColdStartUsers   bool
}

// progress用于向master上报进度
// worker侧离线推荐的主入口：给一批用户生成推荐结果并写入缓存，同时记录进度与监控指标。
func (p *Pipeline) Recommend(ctx context.Context, users []data.User, progress func(completed, throughput int)) {
	startRecommendTime := time.Now()
	itemCache := NewItemCache(p.DataClient)
	log.Logger().Info("ranking recommendation",
		zap.Int("n_working_users", len(users)),
		zap.Int("n_jobs", p.Jobs),
		zap.Int("cache_size", p.Config.Recommend.CacheSize))

	// progress tracker
	completed := make(chan struct{}, 1000)
	_, span := p.Tracer.Start(ctx, "Generate recommendation", len(users))
	defer span.End()

	// 每隔10秒，通过completedCount - previousCount，来计算当前的吞吐量
	go func() {
		defer util.CheckPanic()
		completedCount, previousCount := 0, 0
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case _, ok := <-completed:
				if !ok {
					return
				}
				completedCount++
			case <-ticker.C:
				throughput := completedCount - previousCount
				span.Add(throughput)
				if progress != nil {
					progress(completedCount, completedCount-previousCount)
				}
				previousCount = completedCount
			case <-ctx.Done():
				return
			}
		}
	}()

	// recommendation
	startTime := time.Now()
	var (
		updateUserCount               atomic.Float64
		collaborativeRecommendSeconds atomic.Float64
		userBasedRecommendSeconds     atomic.Float64
		itemBasedRecommendSeconds     atomic.Float64
		latestRecommendSeconds        atomic.Float64
		popularRecommendSeconds       atomic.Float64
	)

	defer MemoryInuseBytesVec.WithLabelValues("user_feedback_cache").Set(0)
	if err := parallel.Detachable(ctx, len(users), p.Jobs, p.Config.OpenAI.ChatCompletionRPM, func(pCtx *parallel.Context, jobId int) {
		defer func() {
			completed <- struct{}{}
		}()
		user := users[jobId]
		userId := user.UserId
		// skip inactive users before max recommend period
		// 如果  用户不活跃 / 不需要重算 ，则不进行重建推荐
		if !p.checkUserActiveTime(ctx, userId) || !p.checkRecommendCacheOutOfDate(ctx, userId) {
			return
		}
		updateUserCount.Add(1)

		recommendTime := time.Now()
		recommender, err := logics.NewRecommender(p.Config.Recommend, p.CacheClient, p.DataClient, false, userId, nil)
		if err != nil {
			log.Logger().Error("failed to create recommender", zap.String("user_id", userId), zap.Error(err))
			return
		}

		// 若允许冷启动且该用户配置了冷启动，则不进行重建推荐
		if !p.dontskipColdStartUsers && recommender.IsColdStart() {
			// skip cold-start users without any positive feedback
			return
		}

		// **CF-MF(矩阵分解)，这里基于master侧算出来的item和user向量结果进行详细检索，写入CF推荐缓存，后面的MF(CF)推荐器会用到
		// MatrixFactorizationUsers和MatrixFactorizationItems是一个worker实例下全局的矩阵分解结果
		// Update collaborative filtering recommendation.
		// 如果Collaborative的type不是none且uesr和item的矩阵分解都存在
		if !strings.EqualFold(p.Config.Recommend.Collaborative.Type, "none") && p.MatrixFactorizationUsers != nil && p.MatrixFactorizationItems != nil {
			if userEmbedding, ok := p.MatrixFactorizationUsers.Get(userId); ok {
				// 有用户向量：更新 CF 推荐缓存；后续仍会继续走其他推荐器
				err = p.updateCollaborativeRecommend(ctx, p.MatrixFactorizationItems, userId, userEmbedding, recommender.ExcludeSet(), itemCache)
				if err != nil {
					log.Logger().Error("failed to recommend by collaborative filtering",
						zap.String("user_id", userId), zap.Error(err))
					return
				}
			} else if !p.dontskipColdStartUsers {
				// 无用户向量且允许跳过冷启动：直接返回，不再走后续推荐流程
				// 可能原因：行为过少或模型尚未生成向量
				// skip users without collaborative filtering embeddings
				return
			}
		}

		// Generate recommendation from recommenders.
		var (
			scores           []cache.Score
			digest           string
			recommenderNames []string
		)
		// 推荐器列表 虽然是Ranker.Recommenders，但实际是召回模块的
		if len(p.Config.Recommend.Ranker.Recommenders) > 0 {
			recommenderNames = p.Config.Recommend.Ranker.Recommenders
		} else {
			// 默认的推荐器列表
			recommenderNames = p.Config.Recommend.ListRecommenders()
		}
		// 按推荐器列表顺序执行，返回候选分数及配置摘要
		// ** scores 召回结果
		scores, digest, err = recommender.RecommendSequential(ctx, scores, 0, recommenderNames...)
		if err != nil {
			log.Logger().Error("failed to recommend items", zap.String("user_id", userId), zap.Error(err))
			return
		}

		// 过滤不存在的物品
		candidates := make([]cache.Score, 0, len(scores))
		candidateSet := mapset.NewSet[string]()
		// 批量拉取候选物品的元数据，用于过滤不存在的物品
		items, err := itemCache.GetMap(ctx, lo.Map(scores, func(score cache.Score, _ int) string {
			return score.Id
		}))
		if err != nil {
			log.Logger().Error("failed to download items", zap.String("user_id", userId), zap.Error(err))
			return
		}
		for _, score := range scores {
			if _, exist := items[score.Id]; exist {
				score.Timestamp = recommendTime
				candidates = append(candidates, score)
				candidateSet.Add(score.Id)
			}
		}

		// 如果启用替换，先把替换候选加入候选集，便于统一排序
		// Insert replacement items into the candidate set before ranking so all rankers (including LLM) can order them.
		var replacementPositiveItems, replacementNegativeItems mapset.Set[string]
		if p.Config.Recommend.Replacement.EnableReplacement && p.Config.Recommend.Ranker.Type != "none" {
			// 追加替换候选，并记录正/负反馈替换集合供后续衰减
			candidates, replacementPositiveItems, replacementNegativeItems, err = p.addReplacementCandidates(
				ctx, candidates, candidateSet, recommender.UserFeedback(), itemCache, recommendTime,
			)
			if err != nil {
				log.Logger().Error("failed to prepare replacement candidates", zap.Error(err))
				return
			}
		}

		// 根据配置选择排序：CTR 模型 / LLM / 原始顺序
		// rank by click-through-rate
		var results []cache.Score
		if p.Config.Recommend.Ranker.Type == "fm" && p.ClickThroughRateModel != nil && !p.ClickThroughRateModel.Invalid() {
			// FM/CTR 排序模型
			results, err = p.rankByClickTroughRate(ctx, p.ClickThroughRateModel, &user, candidates, itemCache, recommendTime)
			if err != nil {
				log.Logger().Error("failed to rank items", zap.Error(err))
				return
			}
		} else if p.Config.Recommend.Ranker.Type == "llm" && p.Config.Recommend.Ranker.Prompt != "" && p.Config.OpenAI.ChatCompletionModel != "" {
			// 基于 LLM 的排序器
			ranker, err := logics.NewChatRanker(p.Config.OpenAI, p.Config.Recommend.Ranker.Prompt)
			if err != nil {
				log.Logger().Error("failed to create LLM ranker", zap.Error(err))
				return
			}
			// 让 LLM 对候选进行重排序
			results, err = p.rankByLLM(ctx, pCtx, ranker, &user, recommender.UserFeedback(), candidates, itemCache, recommendTime)
			if err != nil {
				log.Logger().Error("failed to rank items by LLM", zap.Error(err))
				return
			}
		} else {
			results = candidates
		}

		// 排序后应用替换衰减，避免替换权重绕过排序结果
		// Apply replacement decay after ranking so weights don't bypass the ranker ordering.
		if p.Config.Recommend.Replacement.EnableReplacement && p.Config.Recommend.Ranker.Type != "none" {
			// 对替换项做衰减，避免其权重影响最终排序
			results = p.applyReplacementDecay(results, replacementPositiveItems, replacementNegativeItems)
		}

		// 写入最终推荐结果到缓存
		// cache recommendation
		// 写入最终推荐结果
		if err = p.CacheClient.AddScores(ctx, cache.Recommend, userId, results); err != nil {
			log.Logger().Error("failed to cache recommendation", zap.Error(err))
			return
		}
		// 清理该用户旧的推荐结果（避免累积）
		if err = p.CacheClient.DeleteScores(ctx, []string{cache.Recommend}, cache.ScoreCondition{
			Before: &recommendTime,
			Subset: proto.String(userId),
		}); err != nil {
			log.Logger().Error("failed to delete stale recommendation", zap.Error(err))
			return
		}
		if err = p.CacheClient.Set(ctx,
			cache.Time(cache.Key(cache.RecommendUpdateTime, userId), recommendTime),
			cache.String(cache.Key(cache.RecommendDigest, userId), digest),
		); err != nil {
			log.Logger().Error("failed to cache recommendation time", zap.Error(err))
		}
	}); err != nil {
		log.Logger().Error("recommendation was cancelled", zap.Error(err))
	}
	close(completed)
	log.Logger().Info("complete ranking recommendation",
		zap.String("used_time", time.Since(startTime).String()))
	UpdateUserRecommendTotal.Set(updateUserCount.Load())
	OfflineRecommendTotalSeconds.Set(time.Since(startRecommendTime).Seconds())
	OfflineRecommendStepSecondsVec.WithLabelValues("collaborative_recommend").Set(collaborativeRecommendSeconds.Load())
	OfflineRecommendStepSecondsVec.WithLabelValues("item_based_recommend").Set(itemBasedRecommendSeconds.Load())
	OfflineRecommendStepSecondsVec.WithLabelValues("user_based_recommend").Set(userBasedRecommendSeconds.Load())
	OfflineRecommendStepSecondsVec.WithLabelValues("latest_recommend").Set(latestRecommendSeconds.Load())
	OfflineRecommendStepSecondsVec.WithLabelValues("popular_recommend").Set(popularRecommendSeconds.Load())
}

// checkUserActiveTime checks if a user is active based on their last modification time.
// ActiveUserTTL内有用户行为，视为活跃
func (p *Pipeline) checkUserActiveTime(ctx context.Context, userId string) bool {
	if p.Config.Recommend.ActiveUserTTL == 0 {
		return true
	}
	// read active time
	activeTime, err := p.CacheClient.Get(ctx, cache.Key(cache.LastModifyUserTime, userId)).Time()
	if err != nil {
		log.Logger().Error("failed to read last modify user time", zap.String("user_id", userId), zap.Error(err))
		return true
	}
	if activeTime.IsZero() {
		return true
	}
	// check active time
	if time.Since(activeTime) < time.Duration(p.Config.Recommend.ActiveUserTTL*24)*time.Hour {
		return true
	}
	// remove recommend cache for inactive users
	if err := p.CacheClient.DeleteScores(ctx, []string{cache.Recommend},
		cache.ScoreCondition{Subset: proto.String(userId)}); err != nil {
		log.Logger().Error("failed to delete recommend cache", zap.String("user_id", userId), zap.Error(err))
	}
	return false
}

// checkRecommendCacheOutOfDate checks if recommend cache stale.
// 判断用户的推荐缓存是否过期/需要重算

// 满足以下任意条件，则认为推荐缓存过期/需要重算(return true)：
// 推荐列表为空：SearchScores 返回空
// 推荐摘要为空或与当前配置的 p.Config.Recommend.Hash() 不一致（配置变了）
// 推荐更新时间为空
// 用户最近活跃时间在推荐更新时间之后（说明用户有新行为，缓存旧了）
func (p *Pipeline) checkRecommendCacheOutOfDate(ctx context.Context, userId string) bool {
	var (
		activeTime    time.Time
		recommendTime time.Time
		err           error
	)

	// 1. If cache is empty, stale.
	items, err := p.CacheClient.SearchScores(ctx, cache.Recommend, userId, nil, 0, -1)
	if err != nil {
		log.Logger().Error("failed to load offline recommendation", zap.String("user_id", userId), zap.Error(err))
		return true
	} else if len(items) == 0 {
		return true
	}

	// 2. If digest is empty or not match, stale.
	digest, err := p.CacheClient.Get(ctx, cache.Key(cache.RecommendDigest, userId)).String()
	if err != nil {
		log.Logger().Error("failed to read offline recommendation digest", zap.String("user_id", userId), zap.Error(err))
		return true
	}
	if digest == "" {
		return true
	}
	if digest != p.Config.Recommend.Hash() {
		return true
	}

	// read active time
	activeTime, err = p.CacheClient.Get(ctx, cache.Key(cache.LastModifyUserTime, userId)).Time()
	if err != nil {
		log.Logger().Error("failed to read last modify user time", zap.String("user_id", userId), zap.Error(err))
	}

	// 3. If update time is empty, stale.
	recommendTime, err = p.CacheClient.Get(ctx, cache.Key(cache.RecommendUpdateTime, userId)).Time()
	if err != nil {
		log.Logger().Error("failed to read last update user recommend time", zap.Error(err))
		return true
	}

	// 4. If update time + cache expire > current time, not stale.
	if recommendTime.Before(time.Now().Add(-p.Config.Recommend.CacheExpire)) {
		return true
	}

	// 5. If active time > recommend time, not stale.
	if activeTime.Before(recommendTime) {
		timeoutTime := recommendTime.Add(p.Config.Recommend.Ranker.CacheExpire)
		return timeoutTime.Before(time.Now())
	}
	return true
}

func (p *Pipeline) updateCollaborativeRecommend(
	ctx context.Context,
	items *logics.MatrixFactorizationItems,
	userId string,
	userEmbedding []float32,
	excludeSet mapset.Set[string],
	itemCache *ItemCache,
) error {
	localStartTime := time.Now()

	// 用用户向量在物品向量索引里检索相似物品，先取 CacheSize + excludeSet 数量 的候选，给后面过滤留余量(但实际不会把所有excludeSet都查出来，所以数量会>=CacheSize，但后面用这个recommend的cache的时候是查前CacheSize个)
	scores := items.Search(userEmbedding, p.Config.Recommend.CacheSize+excludeSet.Cardinality())

	// update categories
	// 拉取候选物品的元数据
	itemsMap, err := itemCache.GetMap(ctx, lo.Map(scores, func(score cache.Score, _ int) string {
		return score.Id
	}))
	if err != nil {
		return errors.Trace(err)
	}
	// remove excluded items and non-existing items
	recommend := make([]cache.Score, 0, len(scores))
	for i := range scores {
		// 过滤掉 不存在或者excludeSet中的物品
		if item, exist := itemsMap[scores[i].Id]; exist && !excludeSet.Contains(item.ItemId) {
			recommend = append(recommend, cache.Score{
				Id:         scores[i].Id,
				Score:      scores[i].Score,
				Categories: item.Categories,
				// the scores use the timestamp of the ranking index, which is only refreshed every so often.
				// if we don't overwrite the timestamp here, the code below will delete all scores that were
				// just written.
				Timestamp: localStartTime,
			})
		}
	}

	// add，实际是upsert，比如redis底层用的hset
	if err := p.CacheClient.AddScores(ctx, cache.CollaborativeFiltering, userId, recommend); err != nil {
		log.Logger().Error("failed to cache collaborative filtering recommendation result", zap.String("user_id", userId), zap.Error(err))
		return errors.Trace(err)
	}
	// 记录本次CF的元数据，比如更新时间和哈希，后续判断是否过期或者需要重算
	if err := p.CacheClient.Set(ctx,
		cache.Time(cache.Key(cache.CollaborativeFilteringUpdateTime, userId), localStartTime),
		cache.String(cache.Key(cache.CollaborativeFilteringDigest, userId), p.Config.Recommend.Collaborative.Hash(&p.Config.Recommend)),
	); err != nil {
		log.Logger().Error("failed to cache collaborative filtering recommendation time", zap.String("user_id", userId), zap.Error(err))
		return errors.Trace(err)
	}

	// 清理before localStartTime的cache
	if err := p.CacheClient.DeleteScores(ctx, []string{cache.CollaborativeFiltering}, cache.ScoreCondition{Before: &localStartTime, Subset: proto.String(userId)}); err != nil {
		log.Logger().Error("failed to delete stale collaborative filtering recommendation result", zap.String("user_id", userId), zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// rankByClickTroughRate ranks items by predicted click-through-rate.
func (p *Pipeline) rankByClickTroughRate(
	ctx context.Context,
	predictor ctr.FactorizationMachines,
	user *data.User,
	candidates []cache.Score,
	itemCache *ItemCache,
	recommendTime time.Time,
) ([]cache.Score, error) {
	// download items
	items, err := itemCache.GetSlice(ctx, lo.Map(candidates, func(score cache.Score, _ int) string {
		return score.Id
	}))
	if err != nil {
		return nil, errors.Trace(err)
	}
	// rank by CTR
	topItems := make([]cache.Score, 0, len(items))
	if batchPredictor, ok := predictor.(ctr.BatchInference); ok {
		inputs := make([]lo.Tuple4[string, string, []ctr.Label, []ctr.Label], len(items))
		embeddings := make([][]ctr.Embedding, len(items))
		for i, item := range items {
			inputs[i].A = user.UserId
			inputs[i].B = item.ItemId
			inputs[i].C = ctr.ConvertLabels(user.Labels)
			inputs[i].D = ctr.ConvertLabels(item.Labels)
			embeddings[i] = ctr.ConvertEmbeddings(item.Labels)
		}
		output := batchPredictor.BatchPredict(inputs, embeddings, p.Jobs)
		for i, score := range output {
			topItems = append(topItems, cache.Score{
				Id:         items[i].ItemId,
				Score:      float64(score),
				Categories: items[i].Categories,
				Timestamp:  recommendTime,
			})
		}
	} else {
		for _, item := range items {
			topItems = append(topItems, cache.Score{
				Id:         item.ItemId,
				Score:      float64(predictor.Predict(user.UserId, item.ItemId, ctr.ConvertLabels(user.Labels), ctr.ConvertLabels(item.Labels))),
				Categories: item.Categories,
				Timestamp:  recommendTime,
			})
		}
	}
	cache.SortDocuments(topItems)
	return topItems, nil
}

func (p *Pipeline) rankByLLM(
	ctx context.Context,
	pCtx *parallel.Context,
	ranker *logics.ChatRanker,
	user *data.User,
	feedback []data.Feedback,
	candidates []cache.Score,
	itemCache *ItemCache,
	recommendTime time.Time,
) ([]cache.Score, error) {
	// download items
	items, err := itemCache.GetSlice(ctx, lo.Map(candidates, func(score cache.Score, _ int) string {
		return score.Id
	}))
	if err != nil {
		return nil, errors.Trace(err)
	}
	// convert feedback
	data.SortFeedbacks(feedback)
	contextUserFeedback := make([]data.Feedback, 0, p.Config.Recommend.ContextSize)
	for _, f := range feedback {
		if p.Config.Recommend.ContextSize <= len(contextUserFeedback) {
			break
		}
		if expression.MatchFeedbackTypeExpressions(p.Config.Recommend.DataSource.PositiveFeedbackTypes, f.FeedbackType, f.Value) {
			contextUserFeedback = append(contextUserFeedback, f)
		}
	}
	itemMap, err := itemCache.GetMap(ctx, lo.Map(contextUserFeedback, func(fb data.Feedback, _ int) string {
		return fb.ItemId
	}))
	if err != nil {
		return nil, errors.Trace(err)
	}
	feedbackItems := make([]*logics.FeedbackItem, 0, len(contextUserFeedback))
	for _, fb := range contextUserFeedback {
		if item, exist := itemMap[fb.ItemId]; exist {
			feedbackItems = append(feedbackItems, &logics.FeedbackItem{
				FeedbackType: fb.FeedbackType,
				Item:         *item,
			})
		}
	}
	// rank by LLM
	pCtx.Detach()
	parsed, err := ranker.Rank(ctx, user, feedbackItems, items)
	pCtx.Attach()
	if err != nil {
		return nil, errors.Trace(err)
	}
	// construct scores
	var topItems []cache.Score
	itemCategories := make(map[string][]string, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		itemCategories[item.ItemId] = item.Categories
	}
	for rank, itemId := range parsed {
		topItems = append(topItems, cache.Score{
			Id:         itemId,
			Score:      float64(len(parsed)-rank) / float64(len(parsed)), // normalize score to [0, 1]
			Timestamp:  recommendTime,
			Categories: itemCategories[itemId],
		})
	}
	return topItems, nil
}

// replacement inserts historical items back to recommendation.
// It now adds the replacement items before ranking, then applies decay after ranking.
func (p *Pipeline) addReplacementCandidates(
	ctx context.Context,
	candidates []cache.Score,
	candidateSet mapset.Set[string],
	feedbacks []data.Feedback,
	itemCache *ItemCache,
	recommendTime time.Time,
) ([]cache.Score, mapset.Set[string], mapset.Set[string], error) {
	positiveItems := mapset.NewSet[string]()
	distinctItems := mapset.NewSet[string]()
	for _, feedback := range feedbacks {
		if expression.MatchFeedbackTypeExpressions(p.Config.Recommend.DataSource.PositiveFeedbackTypes, feedback.FeedbackType, feedback.Value) {
			positiveItems.Add(feedback.ItemId)
			distinctItems.Add(feedback.ItemId)
		} else if expression.MatchFeedbackTypeExpressions(p.Config.Recommend.DataSource.ReadFeedbackTypes, feedback.FeedbackType, feedback.Value) {
			distinctItems.Add(feedback.ItemId)
		}
	}

	if distinctItems.Cardinality() == 0 {
		return candidates, positiveItems, mapset.NewSet[string](), nil
	}

	items, err := itemCache.GetSlice(ctx, distinctItems.ToSlice())
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	// Only keep items that exist and aren't hidden in the cache.
	for _, item := range items {
		if candidateSet.Contains(item.ItemId) {
			continue
		}
		candidates = append(candidates, cache.Score{Id: item.ItemId, Categories: item.Categories, Timestamp: recommendTime})
		candidateSet.Add(item.ItemId)
	}

	// Build negative items set from distinct minus positive, filtered by existence.
	existingSet := mapset.NewSet(lo.Map(items, func(it *data.Item, _ int) string { return it.ItemId })...)
	positiveExisting := positiveItems.Intersect(existingSet)
	negativeItems := distinctItems.Difference(positiveItems).Intersect(existingSet)

	return candidates, positiveExisting, negativeItems, nil
}

func (p *Pipeline) applyReplacementDecay(
	results []cache.Score,
	positiveItems mapset.Set[string],
	negativeItems mapset.Set[string],
) []cache.Score {
	if (positiveItems == nil || positiveItems.Cardinality() == 0) && (negativeItems == nil || negativeItems.Cardinality() == 0) {
		return results
	}

	updated := make([]cache.Score, len(results))
	copy(updated, results)
	changed := false
	for i := range updated {
		switch {
		case positiveItems != nil && positiveItems.Contains(updated[i].Id):
			updated[i].Score *= p.Config.Recommend.Replacement.PositiveReplacementDecay
			changed = true
		case negativeItems != nil && negativeItems.Contains(updated[i].Id):
			updated[i].Score *= p.Config.Recommend.Replacement.ReadReplacementDecay
			changed = true
		}
	}
	if changed {
		cache.SortDocuments(updated)
	}
	return updated
}

// ItemCache is alias of map[string]data.Item.
type ItemCache struct {
	Client data.Database
	Data   sync.Map
}

// NewItemCache creates a new ItemCache.
func NewItemCache(client data.Database) *ItemCache {
	return &ItemCache{
		Client: client,
		Data:   sync.Map{},
	}
}

func (c *ItemCache) GetSlice(ctx context.Context, itemIds []string) ([]*data.Item, error) {
	requests := make([]string, 0, len(itemIds))
	for _, itemId := range itemIds {
		if _, exist := c.Data.Load(itemId); !exist {
			requests = append(requests, itemId)
		}
	}
	response, err := c.Client.BatchGetItems(ctx, requests)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, item := range response {
		c.Data.Store(item.ItemId, &item)
	}
	items := make([]*data.Item, 0, len(itemIds))
	for _, itemId := range itemIds {
		if val, exist := c.Data.Load(itemId); exist {
			item := val.(*data.Item)
			if !item.IsHidden {
				items = append(items, item)
			}
		}
	}
	return items, nil
}

func (c *ItemCache) GetMap(ctx context.Context, itemIds []string) (map[string]*data.Item, error) {
	items, err := c.GetSlice(ctx, itemIds)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return lo.SliceToMap(items, func(item *data.Item) (string, *data.Item) {
		return item.ItemId, item
	}), nil
}
