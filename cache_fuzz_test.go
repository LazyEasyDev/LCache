package LCache

import (
	"math"
	"reflect"
	"testing"
)

func FuzzCacheOperationsMatchModel(fuzz *testing.F) {
	fuzz.Add([]byte{})
	fuzz.Add([]byte{0, 0, 3, 0, 0, 0, 5, 4, 2, 0, 5, 0, 5, 0, 0, 60, 7, 0, 0, 0})
	fuzz.Add([]byte{0, 1, 5, 5, 5, 0, 255, 0, 0, 1, 3, 6, 6, 0, 0, 0, 2, 1, 11, 0, 7, 0, 0, 0})
	fuzz.Add([]byte{5, 0, 128, 0, 0, 2, 3, 4, 6, 0, 0, 0, 5, 0, 127, 127, 7, 0, 0, 0, 4, 0, 0, 0})

	fuzz.Fuzz(func(test *testing.T, data []byte) {
		if len(data) > 2048 {
			data = data[:2048]
		}
		type modelEntry struct {
			value         any
			expiresAtUnix int64
		}
		const maximumTTL = int64(120)
		cache, clock := newTestCache(600, Config{MaxTTLSeconds: maximumTTL})
		model := make(map[string]modelEntry)
		keys := []string{"", "first", "second", "third", "\x00", "last"}
		values := []any{nil, false, 42, "value", (*int)(nil), []byte{1, 2}, map[string]int{"number": 42}}
		ttls := []int64{math.MinInt64, -1, 0, 1, 2, 20, 59, 60, 61, 120, 121, math.MaxInt64}
		sourceNowUnix, cachedNowUnix := int64(600), int64(600)

		for offset := 0; offset+3 < len(data); offset += 4 {
			key := keys[int(data[offset+1])%len(keys)]
			ttlSeconds := ttls[int(data[offset+2])%len(ttls)]
			switch data[offset] % 8 {
			case 0, 1:
				value := values[int(data[offset+3])%len(values)]
				cache.Set(key, value, ttlSeconds)
				if ttlSeconds > 0 {
					model[key] = modelEntry{
						value:         value,
						expiresAtUnix: max(sourceNowUnix, 0) + min(ttlSeconds, maximumTTL) + 1,
					}
				}
			case 2:
				record, exists := model[key]
				wantLive := exists && record.expiresAtUnix > cachedNowUnix
				if got := cache.Touch(key, ttlSeconds); got != wantLive {
					test.Fatalf("offset %d: Touch(%q, %d) = %v, want %v", offset, key, ttlSeconds, got, wantLive)
				}
				if wantLive && ttlSeconds > 0 {
					record.expiresAtUnix = max(sourceNowUnix, 0) + min(ttlSeconds, maximumTTL) + 1
					model[key] = record
				}
			case 3:
				record, exists := model[key]
				wantLive := exists && record.expiresAtUnix > cachedNowUnix
				if got := cache.Delete(key); got != wantLive {
					test.Fatalf("offset %d: Delete(%q) = %v, want %v", offset, key, got, wantLive)
				}
				delete(model, key)
			case 4:
				cache.Clear()
				clear(model)
			case 5:
				sourceNowUnix += int64(int8(data[offset+2]))*60 + int64(int8(data[offset+3]))
				clock.Set(sourceNowUnix)
			case 6:
				cachedNowUnix = max(sourceNowUnix, 0)
				cache.state.cachedNowUnix.Store(cachedNowUnix)
			case 7:
				cachedNowUnix = max(sourceNowUnix, 0)
				cache.state.cachedNowUnix.Store(cachedNowUnix)
				safeMinute := cachedNowUnix/60 - 1
				for cache.state.cleanExpiredBatch(safeMinute) {
				}
				for modelKey, record := range model {
					if record.expiresAtUnix/60 <= safeMinute {
						delete(model, modelKey)
					}
				}
			}

			for _, lookupKey := range keys {
				record, exists := model[lookupKey]
				wantFound := exists && record.expiresAtUnix > cachedNowUnix
				value, deadline, remaining, found := cache.GetWithTTL(lookupKey)
				if found != wantFound {
					test.Fatalf("offset %d: GetWithTTL(%q) found = %v, want %v", offset, lookupKey, found, wantFound)
				}
				if wantFound {
					if !reflect.DeepEqual(value, record.value) || deadline != record.expiresAtUnix || remaining != record.expiresAtUnix-cachedNowUnix-1 {
						test.Fatalf("offset %d: GetWithTTL(%q) = (%v, %d, %d), model = %#v, cached time = %d", offset, lookupKey, value, deadline, remaining, record, cachedNowUnix)
					}
				} else if value != nil || deadline != 0 || remaining != 0 {
					test.Fatalf("offset %d: GetWithTTL(%q) returned a nonzero miss", offset, lookupKey)
				}
				if getValue, getFound := cache.Get(lookupKey); getFound != found || !reflect.DeepEqual(getValue, value) {
					test.Fatalf("offset %d: Get(%q) disagrees with GetWithTTL", offset, lookupKey)
				}
			}

			wantCounts := make(map[reflect.Type]int)
			for _, record := range model {
				wantCounts[reflect.TypeOf(record.value)]++
			}
			stats := cache.Stats()
			if stats.Total != len(model) || len(stats.ByType) != len(wantCounts) {
				test.Fatalf("offset %d: stats = %#v, model entries = %d, type counts = %#v", offset, stats, len(model), wantCounts)
			}
			for typ, want := range wantCounts {
				if got := stats.Count(typ); got != want {
					test.Fatalf("offset %d: count for %v = %d, want %d", offset, typ, got, want)
				}
			}
			for index := 1; index < len(stats.ByType); index++ {
				if stats.ByType[index-1].Count < stats.ByType[index].Count {
					test.Fatalf("offset %d: type counts are not in descending order", offset)
				}
			}
			assertIndexInvariant(test, cache)
		}
	})
}

func FuzzExpirationDeadline(fuzz *testing.F) {
	fuzz.Add(int64(0), int64(1))
	fuzz.Add(int64(-1), int64(0))
	fuzz.Add(int64(math.MaxInt64-1), int64(1))
	fuzz.Add(int64(math.MaxInt64-DefaultMaxTTLSeconds), DefaultMaxTTLSeconds)
	fuzz.Add(int64(math.MaxInt64), int64(math.MaxInt64))
	fuzz.Fuzz(func(test *testing.T, nowUnix, ttlSeconds int64) {
		nowUnix = max(nowUnix, 0)
		ttlSeconds = min(max(ttlSeconds, 1), DefaultMaxTTLSeconds)
		want := int64(min(uint64(nowUnix)+uint64(ttlSeconds)+1, uint64(math.MaxInt64)))
		if got := expirationDeadline(nowUnix, ttlSeconds); got != want {
			test.Fatalf("expirationDeadline(%d, %d) = %d, want %d", nowUnix, ttlSeconds, got, want)
		}
	})
}
