package cache

import (
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	now atomic.Int64
}

func newFakeClock(nowUnix int64) *fakeClock {
	clock := &fakeClock{}
	clock.now.Store(nowUnix)
	return clock
}

func (clock *fakeClock) Unix() int64 {
	return clock.now.Load()
}

func (clock *fakeClock) Set(nowUnix int64) {
	clock.now.Store(nowUnix)
}

func newTestCache(nowUnix int64, config Config) (*Cache, *fakeClock) {
	clock := newFakeClock(nowUnix)
	return &Cache{state: newCacheState(config, clock.Unix)}, clock
}

func setTestTime(cache *Cache, clock *fakeClock, nowUnix int64) {
	clock.Set(nowUnix)
	cache.state.cachedNowUnix.Store(nowUnix)
}

func assertIndexInvariant(t *testing.T, cache *Cache) {
	t.Helper()

	state := cache.state
	state.mu.RLock()
	defer state.mu.RUnlock()

	bucketRecords := 0
	for minute, bucket := range state.minuteBuckets {
		if bucket.minute != minute {
			t.Fatalf("bucket key %d contains minute %d", minute, bucket.minute)
		}
		if len(bucket.entries) == 0 {
			t.Fatalf("empty bucket retained for minute %d", minute)
		}
		for key, record := range bucket.entries {
			bucketRecords++
			if unixMinute(record.expiresAtUnix) != minute {
				t.Fatalf("key %q has deadline minute %d in bucket %d", key, unixMinute(record.expiresAtUnix), minute)
			}
			if state.entries[key] != record {
				t.Fatalf("bucket record for %q is not authoritative", key)
			}
		}
	}

	if bucketRecords != len(state.entries) {
		t.Fatalf("bucket records = %d, entries = %d", bucketRecords, len(state.entries))
	}
	wantTypeCounts := make(map[reflect.Type]int)
	for key, record := range state.entries {
		bucket := state.minuteBuckets[unixMinute(record.expiresAtUnix)]
		if bucket == nil || bucket.entries[key] != record {
			t.Fatalf("authoritative record for %q is not indexed", key)
		}
		wantTypeCounts[record.typ]++
	}
	if !reflect.DeepEqual(state.typeCounts, wantTypeCounts) {
		t.Fatalf("type counts = %#v, want %#v", state.typeCounts, wantTypeCounts)
	}
}

func TestDefaultConfigAndNormalization(t *testing.T) {
	if got := DefaultConfig().MaxTTLSeconds; got != DefaultMaxTTLSeconds {
		t.Fatalf("default max TTL = %d, want %d", got, DefaultMaxTTLSeconds)
	}

	tests := []struct {
		name string
		in   int64
		want int64
	}{
		{name: "zero", in: 0, want: DefaultMaxTTLSeconds},
		{name: "negative", in: -1, want: DefaultMaxTTLSeconds},
		{name: "above default", in: DefaultMaxTTLSeconds + 1, want: DefaultMaxTTLSeconds},
		{name: "valid", in: 10, want: 10},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache, _ := newTestCache(100, Config{MaxTTLSeconds: test.in})
			if got := cache.state.maxTTLSeconds; got != test.want {
				t.Fatalf("normalized max TTL = %d, want %d", got, test.want)
			}
		})
	}
}

func TestSetGetAndExpirationBoundary(t *testing.T) {
	cache, clock := newTestCache(100, DefaultConfig())
	cache.Set("", "value", 1)

	value, expiresAtUnix, found := cache.Get("")
	if !found || value != "value" || expiresAtUnix != 102 {
		t.Fatalf("Get before deadline = (%v, %d, %v), want (value, 102, true)", value, expiresAtUnix, found)
	}

	setTestTime(cache, clock, 101)
	if _, _, found := cache.Get(""); !found {
		t.Fatal("entry expired before its deadline")
	}

	setTestTime(cache, clock, 102)
	if value, expiresAtUnix, found := cache.Get(""); found || value != nil || expiresAtUnix != 0 {
		t.Fatalf("Get at deadline = (%v, %d, %v), want miss", value, expiresAtUnix, found)
	}
	assertIndexInvariant(t, cache)
}

func TestSetNonPositiveTTLIsNoOp(t *testing.T) {
	cache, _ := newTestCache(100, DefaultConfig())
	cache.Set("key", "first", 10)
	cache.Set("key", "second", 0)
	cache.Set("missing", "value", -1)

	value, expiresAtUnix, found := cache.Get("key")
	if !found || value != "first" || expiresAtUnix != 111 {
		t.Fatalf("existing entry changed: (%v, %d, %v)", value, expiresAtUnix, found)
	}
	if _, _, found := cache.Get("missing"); found {
		t.Fatal("non-positive TTL inserted a missing key")
	}
	assertIndexInvariant(t, cache)
}

func TestTTLClampAndSaturation(t *testing.T) {
	cache, _ := newTestCache(100, Config{MaxTTLSeconds: 10})
	cache.Set("clamped", 1, 100)
	_, expiresAtUnix, found := cache.Get("clamped")
	if !found || expiresAtUnix != 111 {
		t.Fatalf("clamped deadline = %d, found = %v, want 111 and true", expiresAtUnix, found)
	}

	if got := expirationDeadline(math.MaxInt64-2, 10); got != math.MaxInt64 {
		t.Fatalf("saturated deadline = %d, want %d", got, int64(math.MaxInt64))
	}
	if got := expirationDeadline(math.MaxInt64-2, 1); got != math.MaxInt64 {
		t.Fatalf("boundary deadline = %d, want %d", got, int64(math.MaxInt64))
	}
}

func TestNilValuesAndStats(t *testing.T) {
	type item struct{}

	cache, _ := newTestCache(100, DefaultConfig())
	var typedNil *item
	cache.Set("nil", nil, 60)
	cache.Set("typed-nil", typedNil, 60)
	cache.Set("number", 42, 60)

	value, _, found := cache.Get("nil")
	if !found || value != nil {
		t.Fatalf("bare nil Get = (%v, %v), want (nil, true)", value, found)
	}

	stats := cache.Stats()
	if stats.Total != 3 {
		t.Fatalf("Stats.Total = %d, want 3", stats.Total)
	}
	if stats.ByType[nil] != 1 || stats.ByType[reflect.TypeOf(typedNil)] != 1 || stats.ByType[reflect.TypeOf(42)] != 1 {
		t.Fatalf("unexpected type counts: %#v", stats.ByType)
	}

	stats.ByType[nil] = 99
	if got := cache.Stats().ByType[nil]; got != 1 {
		t.Fatalf("mutating Stats snapshot changed cache count to %d", got)
	}

	cache.Set("number", "forty-two", 60)
	stats = cache.Stats()
	if stats.ByType[reflect.TypeOf(42)] != 0 || stats.ByType[reflect.TypeOf("")] != 1 {
		t.Fatalf("unexpected replacement counts: %#v", stats.ByType)
	}
	assertIndexInvariant(t, cache)
}

func TestTouchMovesBucketAndHonorsExpiration(t *testing.T) {
	cache, clock := newTestCache(100, DefaultConfig())
	cache.Set("key", "value", 20)
	original := cache.state.entries["key"]
	originalMinute := unixMinute(original.expiresAtUnix)

	if !cache.Touch("key", 0) || cache.state.entries["key"] != original {
		t.Fatal("non-positive Touch changed a live entry")
	}

	setTestTime(cache, clock, 200)
	cache.state.cachedNowUnix.Store(100)
	if !cache.Touch("key", 20) {
		t.Fatal("positive Touch returned false for a cached-clock-live entry")
	}
	updated := cache.state.entries["key"]
	if updated == original || updated.expiresAtUnix != 221 {
		t.Fatalf("updated record = %#v, want new pointer with deadline 221", updated)
	}
	if cache.state.minuteBuckets[originalMinute] != nil {
		t.Fatal("old minute bucket remained after Touch")
	}

	setTestTime(cache, clock, 221)
	if cache.Touch("key", 10) {
		t.Fatal("Touch returned true at the expiration deadline")
	}
	if cache.state.entries["key"] != updated {
		t.Fatal("failed Touch physically changed the expired entry")
	}
	assertIndexInvariant(t, cache)
}

func TestDeleteRemovesLiveAndExpiredEntries(t *testing.T) {
	cache, clock := newTestCache(100, DefaultConfig())
	cache.Set("live", "value", 10)
	if !cache.Delete("live") {
		t.Fatal("Delete returned false for a live entry")
	}
	if cache.Delete("live") {
		t.Fatal("Delete returned true for an absent entry")
	}

	cache.Set("expired", "value", 1)
	setTestTime(cache, clock, 102)
	if cache.Delete("expired") {
		t.Fatal("Delete returned true for an expired entry")
	}
	if cache.Stats().Total != 0 {
		t.Fatal("Delete did not physically remove the expired entry")
	}
	assertIndexInvariant(t, cache)
}

func TestSetReplacementMovesBucket(t *testing.T) {
	cache, clock := newTestCache(100, DefaultConfig())
	cache.Set("key", "first", 20)
	first := cache.state.entries["key"]
	firstMinute := unixMinute(first.expiresAtUnix)

	clock.Set(200)
	cache.Set("key", "second", 20)
	second := cache.state.entries["key"]
	secondMinute := unixMinute(second.expiresAtUnix)

	if firstMinute == secondMinute {
		t.Fatal("test setup did not move the entry to another minute")
	}
	if cache.state.minuteBuckets[firstMinute] != nil {
		t.Fatal("old bucket remained after replacement")
	}
	if cache.state.minuteBuckets[secondMinute].entries["key"] != second {
		t.Fatal("replacement was not inserted into its new bucket")
	}
	assertIndexInvariant(t, cache)
}

func TestSameMinuteReplacementAndDeletePreserveBucket(t *testing.T) {
	cache, _ := newTestCache(100, DefaultConfig())
	cache.Set("first", "one", 1)
	cache.Set("second", "two", 10)

	minute := unixMinute(cache.state.entries["first"].expiresAtUnix)
	bucket := cache.state.minuteBuckets[minute]
	if bucket == nil || len(bucket.entries) != 2 {
		t.Fatalf("same-minute bucket contains %d entries, want 2", len(bucket.entries))
	}

	cache.Set("first", "updated", 5)
	if cache.state.minuteBuckets[minute] != bucket || len(bucket.entries) != 2 {
		t.Fatal("same-minute replacement recreated or damaged the shared bucket")
	}
	if !cache.Delete("first") || len(bucket.entries) != 1 || bucket.entries["second"] == nil {
		t.Fatal("deleting one key damaged another key in the same bucket")
	}
	if !cache.Delete("second") || cache.state.minuteBuckets[minute] != nil {
		t.Fatal("last deletion did not remove the empty minute bucket")
	}
	assertIndexInvariant(t, cache)
}

func TestCleanupGraceBoundary(t *testing.T) {
	tests := []struct {
		name       string
		nowUnix    int64
		wantExpiry int64
	}{
		{name: "minute start", nowUnix: 598, wantExpiry: 600},
		{name: "minute second one", nowUnix: 599, wantExpiry: 601},
		{name: "minute end", nowUnix: 657, wantExpiry: 659},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache, _ := newTestCache(test.nowUnix, DefaultConfig())
			cache.Set("key", "value", 1)
			record := cache.state.entries["key"]
			if record.expiresAtUnix != test.wantExpiry {
				t.Fatalf("deadline = %d, want %d", record.expiresAtUnix, test.wantExpiry)
			}

			dueMinute := unixMinute(record.expiresAtUnix)
			cache.state.cleanedMinute = dueMinute - 2
			cache.state.cleanExpiredBatch(dueMinute - 1)
			if cache.Stats().Total != 1 {
				t.Fatal("cleanup removed an entry before its minute became safe")
			}

			cache.state.cleanExpiredBatch(dueMinute)
			if cache.Stats().Total != 0 {
				t.Fatal("cleanup retained an entry after its minute became safe")
			}
			assertIndexInvariant(t, cache)
		})
	}
}

func TestBackwardClockEntryIsNotStrandedBehindCursor(t *testing.T) {
	cache, clock := newTestCache(6000, DefaultConfig())
	if cache.state.cleanedMinute != 99 {
		t.Fatalf("initial cleaned minute = %d, want 99", cache.state.cleanedMinute)
	}

	clock.Set(5700)
	cache.Set("key", "value", 1)
	if cache.state.cleanedMinute != 94 {
		t.Fatalf("cleaned minute after backward-clock Set = %d, want 94", cache.state.cleanedMinute)
	}

	setTestTime(cache, clock, 6000)
	for cache.state.cleanExpiredBatch(95) {
	}
	if cache.Stats().Total != 0 {
		t.Fatal("backward-clock entry remained after the clock recovered")
	}
	assertIndexInvariant(t, cache)
}

func TestCleanExpiredBatchProgress(t *testing.T) {
	cache, _ := newTestCache(600, DefaultConfig())
	cache.state.cleanedMinute = 9
	for index := 0; index < workerBatchSize+1; index++ {
		cache.Set(string(rune(index+1)), index, 1)
	}
	cache.state.cachedNowUnix.Store(900)

	if more := cache.state.cleanExpiredBatch(10); !more {
		t.Fatal("first cleanup batch did not report remaining work")
	}
	if got := cache.Stats().Total; got != 1 {
		t.Fatalf("entries after first batch = %d, want 1", got)
	}
	if cache.state.cleanedMinute != 9 {
		t.Fatalf("cleaned minute advanced across a partial bucket to %d", cache.state.cleanedMinute)
	}

	if more := cache.state.cleanExpiredBatch(10); more {
		t.Fatal("second cleanup batch reported unexpected remaining work")
	}
	if cache.state.cleanedMinute != 10 || cache.Stats().Total != 0 {
		t.Fatalf("cleanup finished with minute %d and total %d", cache.state.cleanedMinute, cache.Stats().Total)
	}
	assertIndexInvariant(t, cache)
}

func TestCleanupIgnoresStaleBucketRecord(t *testing.T) {
	cache, _ := newTestCache(600, DefaultConfig())
	cache.Set("key", "current", 700)
	current := cache.state.entries["key"]

	stale := &entry{value: "stale", typ: reflect.TypeOf(""), expiresAtUnix: 601}
	staleBucket := &minuteBucket{minute: 10, entries: map[string]*entry{"key": stale}}
	cache.state.minuteBuckets[10] = staleBucket
	cache.state.cleanedMinute = 9

	if more := cache.state.cleanExpiredBatch(10); more {
		t.Fatal("cleanup unexpectedly reported more work")
	}
	if cache.state.entries["key"] != current {
		t.Fatal("stale bucket record deleted the current entry")
	}
	assertIndexInvariant(t, cache)
}

func TestCleanupBoundsEmptyMinuteCatchUp(t *testing.T) {
	cache, _ := newTestCache(0, DefaultConfig())
	cache.state.cleanedMinute = 0

	if more := cache.state.cleanExpiredBatch(workerBatchSize + 1); !more {
		t.Fatal("first empty-minute batch did not report remaining work")
	}
	if cache.state.cleanedMinute != workerBatchSize {
		t.Fatalf("cleaned minute = %d, want %d", cache.state.cleanedMinute, workerBatchSize)
	}
	if more := cache.state.cleanExpiredBatch(workerBatchSize + 1); more {
		t.Fatal("second empty-minute batch reported remaining work")
	}
	if cache.state.cleanedMinute != workerBatchSize+1 {
		t.Fatalf("final cleaned minute = %d, want %d", cache.state.cleanedMinute, workerBatchSize+1)
	}
}

func TestCleanupStopsAfterFullBucketBatch(t *testing.T) {
	cache, _ := newTestCache(600, DefaultConfig())
	cache.state.cleanedMinute = 9
	for index := 0; index < workerBatchSize; index++ {
		cache.Set(string(rune(index+1)), index, 1)
	}

	if more := cache.state.cleanExpiredBatch(11); !more {
		t.Fatal("full bucket batch did not report the remaining empty minute")
	}
	if cache.state.cleanedMinute != 10 {
		t.Fatalf("cleaned minute = %d, want 10", cache.state.cleanedMinute)
	}
	if more := cache.state.cleanExpiredBatch(11); more {
		t.Fatal("empty-minute follow-up reported unexpected remaining work")
	}
	if cache.state.cleanedMinute != 11 {
		t.Fatalf("final cleaned minute = %d, want 11", cache.state.cleanedMinute)
	}
}

func TestClearResetsStateAndRemainsReusable(t *testing.T) {
	cache, clock := newTestCache(100, DefaultConfig())
	cache.Set("one", 1, 60)
	cache.Set("two", "two", 60)
	cache.state.cleanedMinute = -1000

	clock.Set(6000)
	cache.Clear()
	if got := cache.Stats(); got.Total != 0 || len(got.ByType) != 0 {
		t.Fatalf("stats after Clear = %#v", got)
	}
	if len(cache.state.minuteBuckets) != 0 {
		t.Fatal("minute buckets remained after Clear")
	}
	if want := unixMinute(6000) - cleanupGraceMinutes; cache.state.cleanedMinute != want {
		t.Fatalf("cleaned minute after Clear = %d, want %d", cache.state.cleanedMinute, want)
	}

	cache.Set("new", "value", 10)
	assertIndexInvariant(t, cache)
	if more := cache.state.cleanExpiredBatch(cache.state.cleanedMinute); more {
		t.Fatal("cleanup reported work at the reset cursor")
	}
	if _, _, found := cache.Get("new"); !found {
		t.Fatal("cache was not reusable after Clear")
	}
}

func TestConcurrentOperationsPreserveIndex(t *testing.T) {
	cache, _ := newTestCache(100, DefaultConfig())

	var waitGroup sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			for iteration := 0; iteration < 500; iteration++ {
				key := string(rune('a' + (worker+iteration)%16))
				cache.Set(key, worker, int64(iteration%60+1))
				cache.Touch(key, int64(iteration%30+1))
				cache.Get(key)
				if iteration%7 == 0 {
					cache.Delete(key)
				}
			}
		}(worker)
	}
	waitGroup.Wait()
	assertIndexInvariant(t, cache)
}

func TestClearRacesWithWritesAndCleanup(t *testing.T) {
	cache, clock := newTestCache(600, DefaultConfig())
	cache.state.cleanedMinute = 9
	for index := 0; index < workerBatchSize+1; index++ {
		cache.Set(string(rune(index+1)), index, 1)
	}
	setTestTime(cache, clock, 900)

	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	waitGroup.Add(3)
	go func() {
		defer waitGroup.Done()
		<-start
		for index := 0; index < 100; index++ {
			cache.state.cleanExpiredBatch(10)
		}
	}()
	go func() {
		defer waitGroup.Done()
		<-start
		for index := 0; index < 500; index++ {
			key := string(rune('a' + index%16))
			cache.Set(key, index, 60)
			if index%5 == 0 {
				cache.Delete(key)
			}
		}
	}()
	go func() {
		defer waitGroup.Done()
		<-start
		for index := 0; index < 50; index++ {
			cache.Clear()
		}
	}()

	close(start)
	waitGroup.Wait()
	cache.Clear()
	cache.Set("after-clear", "value", 60)
	assertIndexInvariant(t, cache)
	if _, _, found := cache.Get("after-clear"); !found {
		t.Fatal("entry inserted after concurrent Clear was lost")
	}
}

func TestRandomizedOperationsMatchModel(t *testing.T) {
	type modelEntry struct {
		value         int
		expiresAtUnix int64
	}

	const maxTTLSeconds = int64(120)
	cache, clock := newTestCache(1000, Config{MaxTTLSeconds: maxTTLSeconds})
	model := make(map[string]modelEntry)
	random := rand.New(rand.NewSource(1))
	nowUnix := int64(1000)

	for step := 0; step < 3000; step++ {
		nowUnix += int64(random.Intn(4))
		setTestTime(cache, clock, nowUnix)
		key := string(rune('a' + random.Intn(12)))
		ttlSeconds := int64(random.Intn(125) - 2)

		switch random.Intn(6) {
		case 0:
			cache.Set(key, step, ttlSeconds)
			if ttlSeconds > 0 {
				if ttlSeconds > maxTTLSeconds {
					ttlSeconds = maxTTLSeconds
				}
				model[key] = modelEntry{value: step, expiresAtUnix: nowUnix + ttlSeconds + 1}
			}
		case 1:
			record, exists := model[key]
			wantFound := exists && record.expiresAtUnix > nowUnix
			if found := cache.Touch(key, ttlSeconds); found != wantFound {
				t.Fatalf("step %d: Touch(%q) = %v, want %v", step, key, found, wantFound)
			}
			if wantFound && ttlSeconds > 0 {
				if ttlSeconds > maxTTLSeconds {
					ttlSeconds = maxTTLSeconds
				}
				record.expiresAtUnix = nowUnix + ttlSeconds + 1
				model[key] = record
			}
		case 2:
			record, exists := model[key]
			wantLive := exists && record.expiresAtUnix > nowUnix
			if live := cache.Delete(key); live != wantLive {
				t.Fatalf("step %d: Delete(%q) = %v, want %v", step, key, live, wantLive)
			}
			delete(model, key)
		case 3:
			record, exists := model[key]
			wantFound := exists && record.expiresAtUnix > nowUnix
			value, expiresAtUnix, found := cache.Get(key)
			if found != wantFound {
				t.Fatalf("step %d: Get(%q) found = %v, want %v", step, key, found, wantFound)
			}
			if wantFound && (value != record.value || expiresAtUnix != record.expiresAtUnix) {
				t.Fatalf("step %d: Get(%q) = (%v, %d), want (%d, %d)", step, key, value, expiresAtUnix, record.value, record.expiresAtUnix)
			}
		case 4:
			safeMinute := unixMinute(nowUnix) - cleanupGraceMinutes
			for cache.state.cleanExpiredBatch(safeMinute) {
			}
			for modelKey, record := range model {
				if unixMinute(record.expiresAtUnix) <= safeMinute {
					delete(model, modelKey)
				}
			}
		case 5:
			cache.Clear()
			clear(model)
		}

		assertIndexInvariant(t, cache)
		if got := cache.Stats().Total; got != len(model) {
			t.Fatalf("step %d: Stats.Total = %d, want %d", step, got, len(model))
		}
	}
}

func TestConcurrentWorkerClearAndMutations(t *testing.T) {
	cache, clock := newTestCache(600, DefaultConfig())
	ticks := make(chan time.Time)
	go cache.state.runWorkerWithTicks(ticks)

	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			<-start
			for iteration := 0; iteration < 500; iteration++ {
				key := string(rune('a' + (worker+iteration)%32))
				cache.Set(key, worker, int64(iteration%120+1))
				if iteration%3 == 0 {
					cache.Touch(key, int64(iteration%60+1))
				}
				if iteration%11 == 0 {
					cache.Delete(key)
				}
				cache.Get(key)
				cache.Stats()
			}
		}(worker)
	}

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		<-start
		for iteration := 0; iteration < 50; iteration++ {
			cache.Clear()
		}
	}()

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		<-start
		for iteration := 1; iteration <= 50; iteration++ {
			clock.Set(600 + int64(iteration)*60)
			ticks <- time.Time{}
		}
	}()

	close(start)
	waitGroup.Wait()
	stopWorker(cache.state)
	select {
	case <-cache.state.workerDone:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after concurrent operations")
	}
	assertIndexInvariant(t, cache)
}

func TestWorkerProcessesTicksAndStops(t *testing.T) {
	cache, clock := newTestCache(600, DefaultConfig())
	cache.state.cleanedMinute = 9
	cache.Set("expired", "value", 1)

	ticks := make(chan time.Time)
	go cache.state.runWorkerWithTicks(ticks)
	clock.Set(900)
	ticks <- time.Time{}

	deadline := time.Now().Add(time.Second)
	for cache.state.cachedNowUnix.Load() != 900 || cache.Stats().Total != 0 {
		if time.Now().After(deadline) {
			t.Fatal("worker did not refresh the clock and clean the due bucket")
		}
		runtime.Gosched()
	}

	stopWorker(cache.state)
	select {
	case <-cache.state.workerDone:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after deterministic tick")
	}
}

func TestWorkerStops(t *testing.T) {
	state := newCacheState(DefaultConfig(), func() int64 { return 100 })
	go state.runWorker()
	stopWorker(state)
	stopWorker(state)

	select {
	case <-state.workerDone:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestNewCache(t *testing.T) {
	cache := New(DefaultConfig())
	cache.Set("key", "value", 60)
	if value, _, found := cache.Get("key"); !found || value != "value" {
		t.Fatalf("New cache Get = (%v, %v), want (value, true)", value, found)
	}

	stopWorker(cache.state)
	select {
	case <-cache.state.workerDone:
	case <-time.After(time.Second):
		t.Fatal("New cache worker did not stop")
	}
}

func TestAutomaticCleanupStopsUnreachableWorker(t *testing.T) {
	workerDone := newUnreachableCacheWorker()
	deadline := time.Now().Add(5 * time.Second)

	for {
		runtime.GC()
		select {
		case <-workerDone:
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("runtime cleanup did not stop the unreachable cache worker")
		}
		runtime.Gosched()
	}
}

func newUnreachableCacheWorker() <-chan struct{} {
	cache := New(DefaultConfig())
	return cache.state.workerDone
}
