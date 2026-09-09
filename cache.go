// Package cache provides a concurrent, in-process cache with per-entry TTLs.
package cache

import (
	"math"
	"reflect"
	"runtime"
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

// Stats is a snapshot of physically resident cache entries.
type Stats struct {
	// Total is the number of physically resident entries, including expired
	// entries that have not yet been removed by background cleanup.
	Total int
	// ByType maps each stored value's dynamic type to its resident entry count.
	// A bare nil value is counted under a nil reflect.Type key.
	ByType map[reflect.Type]int
}

type entry struct {
	value         any
	typ           reflect.Type
	expiresAtUnix int64
}

type minuteBucket struct {
	minute  int64
	entries map[string]*entry
}

type cacheState struct {
	mu            sync.RWMutex
	entries       map[string]*entry
	minuteBuckets map[int64]*minuteBucket
	cleanedMinute int64
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
		minuteBuckets: make(map[int64]*minuteBucket),
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

	record := &entry{
		value:         value,
		typ:           reflect.TypeOf(value),
		expiresAtUnix: expirationDeadline(state.clock(), ttlSeconds),
	}

	if previous, found := state.entries[key]; found {
		state.removeBucketRecord(key, previous)
		state.decrementType(previous.typ)
	}

	state.entries[key] = record
	state.addBucketRecord(key, record)
	state.typeCounts[record.typ]++
}

// Get returns a live value and its absolute Unix-second expiration deadline.
// It returns nil, 0, false when key is absent or expired.
func (c *Cache) Get(key string) (value any, expiresAtUnix int64, found bool) {
	state := c.state
	defer runtime.KeepAlive(c)

	state.mu.RLock()
	record, found := state.entries[key]
	if !found || record.expiresAtUnix <= state.cachedNowUnix.Load() {
		state.mu.RUnlock()
		return nil, 0, false
	}
	value, expiresAtUnix = record.value, record.expiresAtUnix
	state.mu.RUnlock()
	return value, expiresAtUnix, true
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
		typ:           previous.typ,
		expiresAtUnix: expirationDeadline(state.clock(), ttlSeconds),
	}
	state.removeBucketRecord(key, previous)
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
	state.decrementType(record.typ)
	return live
}

// Clear atomically removes all entries and resets statistics. The Cache remains
// usable and its background worker continues running.
func (c *Cache) Clear() {
	state := c.state
	defer runtime.KeepAlive(c)

	state.mu.Lock()
	state.entries = make(map[string]*entry)
	state.minuteBuckets = make(map[int64]*minuteBucket)
	state.typeCounts = make(map[reflect.Type]int)
	state.cleanedMinute = unixMinute(state.clock()) - cleanupGraceMinutes
	state.mu.Unlock()
}

// Stats returns an independent snapshot of physically resident entry counts.
func (c *Cache) Stats() Stats {
	state := c.state
	defer runtime.KeepAlive(c)

	state.mu.RLock()
	stats := Stats{
		Total:  len(state.entries),
		ByType: make(map[reflect.Type]int, len(state.typeCounts)),
	}
	for typ, count := range state.typeCounts {
		stats.ByType[typ] = count
	}
	state.mu.RUnlock()
	return stats
}

func (state *cacheState) addBucketRecord(key string, record *entry) {
	minute := unixMinute(record.expiresAtUnix)
	if minute <= state.cleanedMinute {
		state.cleanedMinute = minute - 1
	}
	bucket := state.minuteBuckets[minute]
	if bucket == nil {
		bucket = &minuteBucket{
			minute:  minute,
			entries: make(map[string]*entry),
		}
		state.minuteBuckets[minute] = bucket
	}
	bucket.entries[key] = record
}

func (state *cacheState) removeBucketRecord(key string, record *entry) {
	minute := unixMinute(record.expiresAtUnix)
	bucket := state.minuteBuckets[minute]
	if bucket == nil || bucket.entries[key] != record {
		return
	}

	delete(bucket.entries, key)
	if len(bucket.entries) == 0 && state.minuteBuckets[minute] == bucket {
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

		for key, record := range bucket.entries {
			if work >= workerBatchSize {
				return true
			}

			delete(bucket.entries, key)
			work++
			if state.entries[key] == record {
				delete(state.entries, key)
				state.decrementType(record.typ)
			}
		}

		if len(bucket.entries) == 0 {
			if state.minuteBuckets[minute] == bucket {
				delete(state.minuteBuckets, minute)
			}
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
	defer ticker.Stop()
	state.runWorkerWithTicks(ticker.C)
}

func (state *cacheState) runWorkerWithTicks(ticks <-chan time.Time) {
	defer close(state.workerDone)

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
				nowUnix := state.clock()
				state.cachedNowUnix.Store(nowUnix)
				safeMinute = unixMinute(nowUnix) - cleanupGraceMinutes
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
			nowUnix := state.clock()
			state.cachedNowUnix.Store(nowUnix)
			safeMinute = unixMinute(nowUnix) - cleanupGraceMinutes
			cleanupPending = state.cleanExpiredBatch(safeMinute)
		}
	}
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
