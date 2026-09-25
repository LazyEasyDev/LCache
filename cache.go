// Package LCache provides a concurrent, in-process cache with per-entry TTLs.
package LCache

import (
	"encoding/json"
	"math"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultMaxTTLSeconds is the default upper bound for entry TTLs: 24 hours.
const DefaultMaxTTLSeconds int64 = 24 * 60 * 60

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
}

// DefaultConfig returns a Config with the default 24-hour maximum TTL.
func DefaultConfig() Config {
	return Config{MaxTTLSeconds: DefaultMaxTTLSeconds}
}

// TypeCount associates a stored value type with its resident entry count.
type TypeCount struct {
	Type  reflect.Type
	Count int
}

// Stats is a snapshot of physically resident cache entries.
type Stats struct {
	// Total is the number of physically resident entries, including expired
	// entries that have not yet been removed by background cleanup.
	Total int
	// ByType contains resident entry counts ordered from highest to lowest.
	// A bare nil value has a nil Type. Equal-count ordering is unspecified.
	ByType []TypeCount
}

// Count returns the resident entry count for typ. It returns zero when typ is
// absent or when Stats is the zero value.
func (s Stats) Count(typ reflect.Type) int {
	for _, typeCount := range s.ByType {
		if typeCount.Type == typ {
			return typeCount.Count
		}
	}
	return 0
}

// ToJSON encodes the Stats snapshot as JSON while preserving ByType order.
// Type names are strings, and the type of a bare nil value is JSON null.
func (s Stats) ToJSON() ([]byte, error) {
	type jsonTypeCount struct {
		Type  *string `json:"type"`
		Count int     `json:"count"`
	}
	type jsonStats struct {
		Total  int             `json:"total"`
		ByType []jsonTypeCount `json:"byType"`
	}

	byType := make([]jsonTypeCount, len(s.ByType))
	for index, typeCount := range s.ByType {
		byType[index].Count = typeCount.Count
		if typeCount.Type != nil {
			typeName := typeCount.Type.String()
			byType[index].Type = &typeName
		}
	}
	return json.Marshal(jsonStats{Total: s.Total, ByType: byType})
}

type entry struct {
	value         any
	expiresAtUnix int64
}

type cacheState struct {
	mu            sync.RWMutex
	entries       map[string]*entry
	minuteBuckets map[int64]map[string]*entry
	cleanedMinute int64
	closed        bool
	typeCounts    map[reflect.Type]int

	cachedNowUnix atomic.Int64
	clock         func() int64
	maxTTLSeconds int64

	stop       chan struct{}
	stopOnce   sync.Once
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

// New creates a Cache and starts its background expiration worker.
func New(config Config) *Cache {
	state := newCacheState(config, func() int64 { return time.Now().Unix() })
	cache := &Cache{state: state}

	runtime.AddCleanup(cache, stopWorker, state)
	go state.runWorker()

	return cache
}

func newCacheState(config Config, clock func() int64) *cacheState {
	maxTTLSeconds := config.MaxTTLSeconds
	if maxTTLSeconds < minimumTTLSeconds || maxTTLSeconds > DefaultMaxTTLSeconds {
		maxTTLSeconds = DefaultMaxTTLSeconds
	}

	sourceClock := clock
	clock = func() int64 {
		return max(sourceClock(), 0)
	}
	nowUnix := clock()
	state := &cacheState{
		entries:       make(map[string]*entry),
		minuteBuckets: make(map[int64]map[string]*entry),
		cleanedMinute: unixMinute(nowUnix) - cleanupGraceMinutes,
		typeCounts:    make(map[reflect.Type]int),
		clock:         clock,
		maxTTLSeconds: maxTTLSeconds,
		stop:          make(chan struct{}),
		workerDone:    make(chan struct{}),
	}
	state.cachedNowUnix.Store(nowUnix)
	return state
}

func stopWorker(state *cacheState) {
	state.stopOnce.Do(func() {
		close(state.stop)
	})
}

// Set stores value under key for ttlSeconds. A non-positive TTL is a no-op,
// and a TTL above the configured maximum is clamped. Set accepts nil values.
// Set is a no-op after Close.
func (c *Cache) Set(key string, value any, ttlSeconds int64) {
	state := c.state
	defer runtime.KeepAlive(c)

	if ttlSeconds <= 0 {
		return
	}
	if ttlSeconds > state.maxTTLSeconds {
		ttlSeconds = state.maxTTLSeconds
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	if state.closed {
		return
	}

	record := &entry{
		value:         value,
		expiresAtUnix: expirationDeadline(state.clock(), ttlSeconds),
	}

	if previous, found := state.entries[key]; found {
		if unixMinute(previous.expiresAtUnix) != unixMinute(record.expiresAtUnix) {
			state.removeBucketRecord(key, previous)
		}
		state.decrementType(reflect.TypeOf(previous.value))
	}

	state.entries[key] = record
	state.addBucketRecord(key, record)
	state.typeCounts[reflect.TypeOf(record.value)]++
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
	state.mu.RLock()
	record, found := state.entries[key]
	if !found {
		state.mu.RUnlock()
		return nil, 0, 0, false
	}
	nowUnix = state.cachedNowUnix.Load()
	if record.expiresAtUnix <= nowUnix {
		state.mu.RUnlock()
		return nil, 0, 0, false
	}
	value, expiresAtUnix = record.value, record.expiresAtUnix
	state.mu.RUnlock()
	return value, expiresAtUnix, nowUnix, true
}

// Touch changes the TTL of a live entry and reports whether one exists. A TTL
// above the configured maximum is clamped. For a live entry, a non-positive
// TTL leaves the deadline unchanged and returns true.
func (c *Cache) Touch(key string, ttlSeconds int64) bool {
	state := c.state
	defer runtime.KeepAlive(c)

	state.mu.Lock()
	defer state.mu.Unlock()

	previous, found := state.entries[key]
	if !found || previous.expiresAtUnix <= state.cachedNowUnix.Load() {
		return false
	}
	if ttlSeconds <= 0 {
		return true
	}
	if ttlSeconds > state.maxTTLSeconds {
		ttlSeconds = state.maxTTLSeconds
	}

	record := &entry{
		value:         previous.value,
		expiresAtUnix: expirationDeadline(state.clock(), ttlSeconds),
	}
	if unixMinute(previous.expiresAtUnix) != unixMinute(record.expiresAtUnix) {
		state.removeBucketRecord(key, previous)
	}
	state.entries[key] = record
	state.addBucketRecord(key, record)
	return true
}

// Delete physically removes key and reports whether its entry was live. An
// expired entry may be removed while Delete returns false.
func (c *Cache) Delete(key string) bool {
	state := c.state
	defer runtime.KeepAlive(c)

	state.mu.Lock()
	defer state.mu.Unlock()

	record, found := state.entries[key]
	if !found {
		return false
	}

	live := record.expiresAtUnix > state.cachedNowUnix.Load()
	delete(state.entries, key)
	state.removeBucketRecord(key, record)
	state.decrementType(reflect.TypeOf(record.value))
	return live
}

// Clear atomically removes all entries and resets statistics. The Cache remains
// usable and its background worker continues running. Clear is a no-op after Close.
func (c *Cache) Clear() {
	state := c.state
	defer runtime.KeepAlive(c)

	state.mu.Lock()
	defer state.mu.Unlock()

	if state.closed {
		return
	}

	state.entries = make(map[string]*entry)
	state.minuteBuckets = make(map[int64]map[string]*entry)
	state.typeCounts = make(map[reflect.Type]int)
	state.cleanedMinute = unixMinute(state.clock()) - cleanupGraceMinutes
}

// Stats returns an independent snapshot of physically resident entry counts.
func (c *Cache) Stats() Stats {
	state := c.state
	defer runtime.KeepAlive(c)

	state.mu.RLock()
	stats := Stats{
		Total:  len(state.entries),
		ByType: make([]TypeCount, 0, len(state.typeCounts)),
	}
	for typ, count := range state.typeCounts {
		stats.ByType = append(stats.ByType, TypeCount{Type: typ, Count: count})
	}
	state.mu.RUnlock()
	sort.Slice(stats.ByType, func(left, right int) bool {
		return stats.ByType[left].Count > stats.ByType[right].Count
	})
	return stats
}

// Close permanently empties the Cache and waits for its background worker to stop.
// After Close, reads miss, Set and Clear are no-ops, Touch and Delete return false,
// and Stats is empty. Close is safe to call repeatedly and concurrently with all
// other operations. A closed Cache cannot be reopened.
func (c *Cache) Close() {
	state := c.state
	defer runtime.KeepAlive(c)

	state.mu.Lock()
	state.closed = true
	state.entries = nil
	state.minuteBuckets = nil
	state.typeCounts = nil
	state.mu.Unlock()

	stopWorker(state)
	<-state.workerDone
}

func (state *cacheState) addBucketRecord(key string, record *entry) {
	minute := unixMinute(record.expiresAtUnix)
	if minute <= state.cleanedMinute {
		state.cleanedMinute = minute - 1
	}
	bucket := state.minuteBuckets[minute]
	if bucket == nil {
		bucket = make(map[string]*entry)
		state.minuteBuckets[minute] = bucket
	}
	bucket[key] = record
}

func (state *cacheState) removeBucketRecord(key string, record *entry) {
	minute := unixMinute(record.expiresAtUnix)
	bucket := state.minuteBuckets[minute]
	if bucket[key] != record {
		return
	}

	delete(bucket, key)
	if len(bucket) == 0 {
		delete(state.minuteBuckets, minute)
	}
}

func (state *cacheState) decrementType(typ reflect.Type) {
	count := state.typeCounts[typ]
	if count <= 1 {
		delete(state.typeCounts, typ)
		return
	}
	state.typeCounts[typ] = count - 1
}

func (state *cacheState) cleanExpiredBatch(safeMinute int64) bool {
	state.mu.Lock()
	defer state.mu.Unlock()

	work := 0
	for state.cleanedMinute < safeMinute {
		minute := state.cleanedMinute + 1
		bucket := state.minuteBuckets[minute]
		if bucket == nil {
			state.cleanedMinute = minute
			work++
			if work >= workerBatchSize {
				return state.cleanedMinute < safeMinute
			}
			continue
		}

		for key, record := range bucket {
			if work >= workerBatchSize {
				return true
			}

			delete(bucket, key)
			work++
			if state.entries[key] == record {
				delete(state.entries, key)
				state.decrementType(reflect.TypeOf(record.value))
			}
		}

		if len(bucket) == 0 {
			delete(state.minuteBuckets, minute)
			state.cleanedMinute = minute
		}
		if work >= workerBatchSize {
			return state.cleanedMinute < safeMinute
		}
	}

	return false
}

func (state *cacheState) runWorker() {
	ticker := time.NewTicker(clockUpdateInterval)
	defer close(state.workerDone)
	defer ticker.Stop()
	state.runWorkerWithTicks(ticker.C)
}

func (state *cacheState) runWorkerWithTicks(ticks <-chan time.Time) {
	var safeMinute int64
	cleanupPending := false

	for {
		if cleanupPending {
			select {
			case <-state.stop:
				return
			default:
			}

			select {
			case <-state.stop:
				return
			case <-ticks:
				safeMinute = state.refreshClock()
			default:
			}

			cleanupPending = state.cleanExpiredBatch(safeMinute)
			runtime.Gosched()
			continue
		}

		select {
		case <-state.stop:
			return
		case <-ticks:
			safeMinute = state.refreshClock()
			cleanupPending = state.cleanExpiredBatch(safeMinute)
		}
	}
}

func (state *cacheState) refreshClock() int64 {
	nowUnix := state.clock()
	state.cachedNowUnix.Store(nowUnix)
	return unixMinute(nowUnix) - cleanupGraceMinutes
}

func unixMinute(unixSeconds int64) int64 {
	return unixSeconds / secondsPerMinute
}

func expirationDeadline(nowUnix, ttlSeconds int64) int64 {
	if nowUnix > math.MaxInt64-ttlSeconds {
		return math.MaxInt64
	}
	deadline := nowUnix + ttlSeconds
	if deadline == math.MaxInt64 {
		return math.MaxInt64
	}
	return deadline + 1
}
