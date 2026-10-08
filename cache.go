// Package LCache provides a concurrent, in-process cache with per-entry TTLs.
package LCache

import (
	"encoding/json"
	"hash/maphash"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultMaxTTLSeconds is the default upper bound for entry TTLs: 24 hours.
const DefaultMaxTTLSeconds int64 = 24 * 60 * 60

// DefaultShardCount is the default number of independently locked key maps.
const DefaultShardCount = 32

// MaxShardCount is the largest supported shard count.
const MaxShardCount = 1_024

const (
	clockUpdateInterval = time.Second
	workerBatchSize     = 4 * 1024
	cleanupGraceMinutes = int64(1)
	minimumTTLSeconds   = int64(1)
	secondsPerMinute    = int64(60)
)

// Config controls Cache behavior.
type Config struct {
	// MaxTTLSeconds is the largest TTL accepted by Set and Touch. Values outside
	// the range 1 through DefaultMaxTTLSeconds use DefaultMaxTTLSeconds.
	MaxTTLSeconds int64
	// ShardCount is the number of independently locked key maps. Values outside
	// the range 1 through MaxShardCount use DefaultShardCount. One disables sharding.
	ShardCount int
}

// DefaultConfig returns a Config with the default maximum TTL and shard count.
func DefaultConfig() Config {
	return Config{
		MaxTTLSeconds: DefaultMaxTTLSeconds,
		ShardCount:    DefaultShardCount,
	}
}

// TagCount associates a caller-defined tag with its resident entry count.
type TagCount struct {
	Tag   string `json:"tag"`
	Count int    `json:"count"`
}

// Stats is a snapshot of physically resident cache entries.
type Stats struct {
	// Total is the number of physically resident entries, including expired
	// entries that have not yet been removed by background cleanup.
	Total int `json:"total"`
	// ByTag contains resident entry counts ordered from highest to lowest.
	// The empty string is a valid tag. Equal-count ordering is unspecified.
	ByTag []TagCount `json:"byTag"`
}

// Count returns the resident entry count for tag. It returns zero when tag is
// absent or when Stats is the zero value.
func (s Stats) Count(tag string) int {
	for _, tagCount := range s.ByTag {
		if tagCount.Tag == tag {
			return tagCount.Count
		}
	}
	return 0
}

// ToJSON encodes the Stats snapshot as JSON while preserving ByTag order.
// An empty snapshot encodes ByTag as an empty array rather than null.
func (s Stats) ToJSON() ([]byte, error) {
	if s.ByTag == nil {
		s.ByTag = []TagCount{}
	}
	return json.Marshal(s)
}

type entry struct {
	value         any
	expiresAtUnix int64
	tag           string
}

type cacheShard struct {
	mu            sync.RWMutex
	entries       map[string]*entry
	minuteBuckets map[int64]map[string]*entry
	cleanedMinute int64
	closed        bool
	tagCounts     map[string]int
}

type cacheState struct {
	shards        []cacheShard
	hashSeed      maphash.Seed
	cachedNowUnix atomic.Int64
	maxTTLSeconds int64

	stop       chan struct{}
	stopOnce   sync.Once
	clockDone  chan struct{}
	workerDone chan struct{}
}

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// Cache is a concurrent, in-process cache for values of any type.
// A Cache must not be copied after New returns; share it through a *Cache.
type Cache struct {
	noCopy noCopy
	state  *cacheState
}

// New creates a Cache and starts its background clock and expiration workers.
func New(config Config) *Cache {
	maxTTLSeconds := config.MaxTTLSeconds
	if maxTTLSeconds < minimumTTLSeconds || maxTTLSeconds > DefaultMaxTTLSeconds {
		maxTTLSeconds = DefaultMaxTTLSeconds
	}
	shardCount := config.ShardCount
	if shardCount < 1 || shardCount > MaxShardCount {
		shardCount = DefaultShardCount
	}

	nowUnix := max(time.Now().Unix(), 0)
	state := &cacheState{
		shards:        make([]cacheShard, shardCount),
		hashSeed:      maphash.MakeSeed(),
		maxTTLSeconds: maxTTLSeconds,
		stop:          make(chan struct{}),
		clockDone:     make(chan struct{}),
		workerDone:    make(chan struct{}),
	}
	for index := range state.shards {
		shard := &state.shards[index]
		shard.entries = make(map[string]*entry)
		shard.minuteBuckets = make(map[int64]map[string]*entry)
		shard.cleanedMinute = unixMinute(nowUnix) - cleanupGraceMinutes
		shard.tagCounts = make(map[string]int)
	}
	state.cachedNowUnix.Store(nowUnix)
	cache := &Cache{state: state}

	runtime.AddCleanup(cache, stopWorker, state)
	go state.runClockUpdate()
	go state.runWorker()

	return cache
}

func (state *cacheState) shardFor(key string) *cacheShard {
	if len(state.shards) == 1 {
		return &state.shards[0]
	}
	index := maphash.String(state.hashSeed, key) % uint64(len(state.shards))
	return &state.shards[index]
}

func (state *cacheState) lockShards() {
	for index := range state.shards {
		state.shards[index].mu.Lock()
	}
}

func (state *cacheState) unlockShards() {
	for index := len(state.shards) - 1; index >= 0; index-- {
		state.shards[index].mu.Unlock()
	}
}

func stopWorker(state *cacheState) {
	state.stopOnce.Do(func() {
		close(state.stop)
	})
}

// Set stores value under key for ttlSeconds and groups it under tag in Stats.
// A non-positive TTL is a no-op, and a TTL above the configured maximum is
// clamped. Set accepts nil values and empty tags. Replacing a key also replaces
// its tag. Set is a no-op after Close.
func (c *Cache) Set(key string, value any, ttlSeconds int64, tag string) {
	state := c.state
	defer runtime.KeepAlive(c)

	if ttlSeconds <= 0 {
		return
	}
	ttlSeconds = min(ttlSeconds, state.maxTTLSeconds)

	shard := state.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if shard.closed {
		return
	}

	record := &entry{
		value:         value,
		expiresAtUnix: expirationDeadline(state.cachedNowUnix.Load(), ttlSeconds),
		tag:           tag,
	}

	if previous, found := shard.entries[key]; found {
		if unixMinute(previous.expiresAtUnix) != unixMinute(record.expiresAtUnix) {
			shard.removeBucketRecord(key, previous)
		}
		if previous.tag != tag {
			shard.decrementTag(previous.tag)
			shard.tagCounts[tag]++
		}
	} else {
		shard.tagCounts[tag]++
	}

	shard.entries[key] = record
	shard.addBucketRecord(key, record)
}

// Get returns the live value stored under key. It returns nil, false when key
// is absent or expired.
func (c *Cache) Get(key string) (value any, found bool) {
	state := c.state
	defer runtime.KeepAlive(c)

	value, _, _, found = state.get(key)
	return value, found
}

// GetWithTTL returns the live value stored under key, its absolute Unix-second
// expiration deadline, and its remaining complete TTL seconds. The remaining
// TTL may be zero while the entry is live in its final partial second. It
// returns nil, 0, 0, false when key is absent or expired.
func (c *Cache) GetWithTTL(key string) (value any, expiresAtUnix, remainingTTLSeconds int64, found bool) {
	state := c.state
	defer runtime.KeepAlive(c)

	value, expiresAtUnix, nowUnix, found := state.get(key)
	if !found {
		return nil, 0, 0, false
	}
	return value, expiresAtUnix, expiresAtUnix - nowUnix - 1, true
}

func (state *cacheState) get(key string) (value any, expiresAtUnix, nowUnix int64, found bool) {
	shard := state.shardFor(key)
	shard.mu.RLock()
	record, found := shard.entries[key]
	if !found {
		shard.mu.RUnlock()
		return nil, 0, 0, false
	}
	nowUnix = state.cachedNowUnix.Load()
	if record.expiresAtUnix <= nowUnix {
		shard.mu.RUnlock()
		return nil, 0, 0, false
	}
	value, expiresAtUnix = record.value, record.expiresAtUnix
	shard.mu.RUnlock()
	return value, expiresAtUnix, nowUnix, true
}

// Touch changes the TTL of a live entry and reports whether one exists. A TTL
// above the configured maximum is clamped. For a live entry, a non-positive
// TTL leaves the deadline unchanged and returns true. Touch preserves the tag.
func (c *Cache) Touch(key string, ttlSeconds int64) bool {
	state := c.state
	defer runtime.KeepAlive(c)

	shard := state.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	previous, found := shard.entries[key]
	nowUnix := state.cachedNowUnix.Load()
	if !found || previous.expiresAtUnix <= nowUnix {
		return false
	}
	if ttlSeconds <= 0 {
		return true
	}
	ttlSeconds = min(ttlSeconds, state.maxTTLSeconds)

	record := &entry{
		value:         previous.value,
		expiresAtUnix: expirationDeadline(nowUnix, ttlSeconds),
		tag:           previous.tag,
	}
	if unixMinute(previous.expiresAtUnix) != unixMinute(record.expiresAtUnix) {
		shard.removeBucketRecord(key, previous)
	}
	shard.entries[key] = record
	shard.addBucketRecord(key, record)
	return true
}

// Delete physically removes key and reports whether its entry was live. An
// expired entry may be removed while Delete returns false.
func (c *Cache) Delete(key string) bool {
	state := c.state
	defer runtime.KeepAlive(c)

	shard := state.shardFor(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	record, found := shard.entries[key]
	if !found {
		return false
	}

	live := record.expiresAtUnix > state.cachedNowUnix.Load()
	delete(shard.entries, key)
	shard.removeBucketRecord(key, record)
	shard.decrementTag(record.tag)
	return live
}

// Clear atomically removes all entries and resets statistics. The Cache remains
// usable and its background workers continue running. Clear is a no-op after Close.
func (c *Cache) Clear() {
	state := c.state
	defer runtime.KeepAlive(c)

	state.lockShards()
	defer state.unlockShards()

	if state.shards[0].closed {
		return
	}

	cleanedMinute := unixMinute(state.cachedNowUnix.Load()) - cleanupGraceMinutes
	for index := range state.shards {
		shard := &state.shards[index]
		shard.entries = make(map[string]*entry)
		shard.minuteBuckets = make(map[int64]map[string]*entry)
		shard.tagCounts = make(map[string]int)
		shard.cleanedMinute = cleanedMinute
	}
}

// Stats returns an independent snapshot of physically resident entry counts.
func (c *Cache) Stats() Stats {
	state := c.state
	defer runtime.KeepAlive(c)

	for index := range state.shards {
		state.shards[index].mu.RLock()
	}
	stats := Stats{}
	tagCounts := make(map[string]int)
	for index := range state.shards {
		shard := &state.shards[index]
		stats.Total += len(shard.entries)
		for tag, count := range shard.tagCounts {
			tagCounts[tag] += count
		}
	}
	for index := len(state.shards) - 1; index >= 0; index-- {
		state.shards[index].mu.RUnlock()
	}
	stats.ByTag = make([]TagCount, 0, len(tagCounts))
	for tag, count := range tagCounts {
		stats.ByTag = append(stats.ByTag, TagCount{Tag: tag, Count: count})
	}
	sort.Slice(stats.ByTag, func(left, right int) bool {
		return stats.ByTag[left].Count > stats.ByTag[right].Count
	})
	return stats
}

// Close permanently empties the Cache and waits for its background workers to stop.
// After Close, reads miss, Set and Clear are no-ops, Touch and Delete return false,
// and Stats is empty. Close is safe to call repeatedly and concurrently with all
// other operations. A closed Cache cannot be reopened.
func (c *Cache) Close() {
	state := c.state
	defer runtime.KeepAlive(c)

	stopWorker(state)
	<-state.clockDone
	<-state.workerDone

	state.lockShards()
	for index := range state.shards {
		shard := &state.shards[index]
		shard.closed = true
		shard.entries = nil
		shard.minuteBuckets = nil
		shard.tagCounts = nil
	}
	state.unlockShards()
}

func (shard *cacheShard) addBucketRecord(key string, record *entry) {
	minute := unixMinute(record.expiresAtUnix)
	if minute <= shard.cleanedMinute {
		shard.cleanedMinute = minute - 1
	}
	bucket := shard.minuteBuckets[minute]
	if bucket == nil {
		bucket = make(map[string]*entry)
		shard.minuteBuckets[minute] = bucket
	}
	bucket[key] = record
}

func (shard *cacheShard) removeBucketRecord(key string, record *entry) {
	minute := unixMinute(record.expiresAtUnix)
	bucket := shard.minuteBuckets[minute]
	if bucket[key] != record {
		return
	}

	delete(bucket, key)
	if len(bucket) == 0 {
		delete(shard.minuteBuckets, minute)
	}
}

func (shard *cacheShard) decrementTag(tag string) {
	count := shard.tagCounts[tag]
	if count <= 1 {
		delete(shard.tagCounts, tag)
		return
	}
	shard.tagCounts[tag] = count - 1
}

func (state *cacheState) cleanExpired() {
	safeMinute := unixMinute(state.cachedNowUnix.Load()) - cleanupGraceMinutes
	for index := range state.shards {
		shard := &state.shards[index]
		for {
			select {
			case <-state.stop:
				return
			default:
			}

			shard.mu.Lock()
			if shard.cleanedMinute >= safeMinute {
				shard.mu.Unlock()
				break
			}
			shard.cleanExpiredBatchLocked(safeMinute)
			shard.mu.Unlock()
			runtime.Gosched()
		}
	}
}

func (shard *cacheShard) cleanExpiredBatchLocked(safeMinute int64) {
	work := 0
	for shard.cleanedMinute < safeMinute && work < workerBatchSize {
		minute := shard.cleanedMinute + 1
		bucket := shard.minuteBuckets[minute]
		if bucket == nil {
			shard.cleanedMinute = minute
			work++
			continue
		}

		for key, record := range bucket {
			if work >= workerBatchSize {
				break
			}

			delete(bucket, key)
			work++
			if shard.entries[key] == record {
				delete(shard.entries, key)
				shard.decrementTag(record.tag)
			}
		}

		if len(bucket) == 0 {
			delete(shard.minuteBuckets, minute)
			shard.cleanedMinute = minute
		}
	}
}

func (state *cacheState) runClockUpdate() {
	ticker := time.NewTicker(clockUpdateInterval)
	defer close(state.clockDone)
	defer ticker.Stop()

	for {
		select {
		case <-state.stop:
			return
		case <-ticker.C:
			state.refreshClock()
		}
	}
}

func (state *cacheState) runWorker() {
	ticker := time.NewTicker(time.Minute)
	defer close(state.workerDone)
	defer ticker.Stop()

	for {
		select {
		case <-state.stop:
			return
		case <-ticker.C:
			state.cleanExpired()
		}
	}
}

func (state *cacheState) refreshClock() {
	nowUnix := max(time.Now().Unix(), 0)
	state.cachedNowUnix.Store(nowUnix)
}

func unixMinute(unixSeconds int64) int64 {
	return unixSeconds / secondsPerMinute
}

func expirationDeadline(nowUnix, ttlSeconds int64) int64 {
	if nowUnix >= math.MaxInt64-ttlSeconds {
		return math.MaxInt64
	}
	return nowUnix + ttlSeconds + 1
}
