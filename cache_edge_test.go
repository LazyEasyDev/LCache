package LCache

import (
	"math"
	"reflect"
	"testing"
)

func TestExpirationDeadlineBoundaries(test *testing.T) {
	scenarios := []struct {
		name       string
		nowUnix    int64
		ttlSeconds int64
		want       int64
	}{
		{name: "epoch", nowUnix: 0, ttlSeconds: 1, want: 2},
		{name: "ordinary", nowUnix: 100, ttlSeconds: 10, want: 111},
		{name: "below saturation", nowUnix: math.MaxInt64 - 3, ttlSeconds: 1, want: math.MaxInt64 - 1},
		{name: "rounding reaches limit", nowUnix: math.MaxInt64 - 2, ttlSeconds: 1, want: math.MaxInt64},
		{name: "sum reaches limit", nowUnix: math.MaxInt64 - 1, ttlSeconds: 1, want: math.MaxInt64},
		{name: "sum exceeds limit", nowUnix: math.MaxInt64 - 1, ttlSeconds: 2, want: math.MaxInt64},
		{name: "clock at limit", nowUnix: math.MaxInt64, ttlSeconds: 1, want: math.MaxInt64},
		{name: "maximum TTL", nowUnix: math.MaxInt64 - DefaultMaxTTLSeconds, ttlSeconds: DefaultMaxTTLSeconds, want: math.MaxInt64},
	}

	for _, scenario := range scenarios {
		test.Run(scenario.name, func(test *testing.T) {
			if got := expirationDeadline(scenario.nowUnix, scenario.ttlSeconds); got != scenario.want {
				test.Fatalf("expirationDeadline(%d, %d) = %d, want %d", scenario.nowUnix, scenario.ttlSeconds, got, scenario.want)
			}
		})
	}
}

func TestRemoveBucketRecordIgnoresNonmatchingEntry(test *testing.T) {
	scenarios := []struct {
		name          string
		key           string
		expiresAtUnix int64
	}{
		{name: "missing bucket", key: "key", expiresAtUnix: 660},
		{name: "missing key", key: "missing", expiresAtUnix: 602},
		{name: "stale entry", key: "key", expiresAtUnix: 602},
	}

	for _, scenario := range scenarios {
		test.Run(scenario.name, func(test *testing.T) {
			cache, _ := newTestCache(600, DefaultConfig())
			cache.Set("key", "current", 1)
			stale := &entry{expiresAtUnix: scenario.expiresAtUnix}

			cache.state.mu.Lock()
			cache.state.removeBucketRecord(scenario.key, stale)
			cache.state.mu.Unlock()

			if value, found := cache.Get("key"); !found || value != "current" {
				test.Fatalf("Get after stale removal = (%v, %v), want (current, true)", value, found)
			}
			assertIndexInvariant(test, cache)
		})
	}
}

func TestTypeCountsAcrossEntryLifecycle(test *testing.T) {
	values := []struct {
		name  string
		value any
	}{
		{name: "bare nil", value: nil},
		{name: "typed nil", value: (*int)(nil)},
		{name: "integer", value: 42},
		{name: "string", value: "value"},
		{name: "slice", value: []byte{1, 2}},
		{name: "map", value: map[string]int{"number": 42}},
		{name: "pointer", value: new(int)},
	}

	for _, stored := range values {
		test.Run(stored.name, func(test *testing.T) {
			cache, clock := newTestCache(600, DefaultConfig())
			cache.Set("other", stored.value, 60)
			cache.Set("key", "original", 1)
			cache.Set("key", stored.value, 20)

			stats := cache.Stats()
			if stats.Total != 2 || len(stats.ByType) != 1 || stats.Count(reflect.TypeOf(stored.value)) != 2 {
				test.Fatalf("stats after replacement = %#v, want two entries of type %v", stats, reflect.TypeOf(stored.value))
			}
			if value, found := cache.Get("key"); !found || !reflect.DeepEqual(value, stored.value) {
				test.Fatalf("Get = (%v, %v), want (%v, true)", value, found, stored.value)
			}
			assertIndexInvariant(test, cache)

			if !cache.Touch("key", 30) {
				test.Fatal("Touch missed a live entry")
			}
			assertIndexInvariant(test, cache)
			if !cache.Delete("other") {
				test.Fatal("Delete missed a live entry")
			}
			assertIndexInvariant(test, cache)

			setTestTime(cache, clock, 660)
			if _, found := cache.Get("key"); found {
				test.Fatal("Get returned an expired entry")
			}
			if count := cache.Stats().Count(reflect.TypeOf(stored.value)); count != 1 {
				test.Fatalf("resident count before cleanup = %d, want 1", count)
			}
			if more := cache.state.cleanExpiredBatch(10); more {
				test.Fatal("cleanup unexpectedly left pending work")
			}
			if stats := cache.Stats(); stats.Total != 0 || len(stats.ByType) != 0 {
				test.Fatalf("stats after cleanup = %#v, want empty", stats)
			}
			assertIndexInvariant(test, cache)
		})
	}
}

func TestTouchTTLNormalization(test *testing.T) {
	scenarios := []struct {
		name          string
		ttlSeconds    int64
		wantExpiresAt int64
	}{
		{name: "negative", ttlSeconds: math.MinInt64, wantExpiresAt: 603},
		{name: "zero", ttlSeconds: 0, wantExpiresAt: 603},
		{name: "shorten", ttlSeconds: 1, wantExpiresAt: 602},
		{name: "maximum", ttlSeconds: 10, wantExpiresAt: 611},
		{name: "clamped", ttlSeconds: math.MaxInt64, wantExpiresAt: 611},
	}

	for _, scenario := range scenarios {
		test.Run(scenario.name, func(test *testing.T) {
			cache, _ := newTestCache(600, Config{MaxTTLSeconds: 10})
			cache.Set("key", "value", 2)
			previous := cache.state.entries["key"]
			bucket := cache.state.minuteBuckets[10]

			if !cache.Touch("key", scenario.ttlSeconds) {
				test.Fatal("Touch missed a live entry")
			}
			if _, deadline, _, found := cache.GetWithTTL("key"); !found || deadline != scenario.wantExpiresAt {
				test.Fatalf("Touch deadline = %d, found = %v, want %d, true", deadline, found, scenario.wantExpiresAt)
			}
			updated := cache.state.entries["key"]
			if (updated == previous) != (scenario.ttlSeconds <= 0) {
				test.Fatal("Touch did not preserve the entry for a no-op or replace it for a positive TTL")
			}
			if len(bucket) != 1 || bucket["key"] != updated {
				test.Fatal("Touch did not reuse the same-minute bucket")
			}
			assertIndexInvariant(test, cache)
		})
	}
}
