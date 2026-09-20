package cache

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestCloseStopsWorkerAndEmptiesCache(test *testing.T) {
	cache := New(DefaultConfig())
	test.Cleanup(cache.Close)
	cache.Set("key", "value", 60)
	cache.Set("nil", nil, 60)
	cache.Set("self", cache, 60)

	cache.Close()

	select {
	case <-cache.state.workerDone:
	default:
		test.Fatal("Close returned before the worker stopped")
	}
	assertClosedCache(test, cache)
}

func TestCloseIsIdempotent(test *testing.T) {
	for _, name := range []string{"empty", "populated", "worker-stopped"} {
		test.Run(name, func(test *testing.T) {
			cache := New(DefaultConfig())
			test.Cleanup(cache.Close)
			if name != "empty" {
				cache.Set("key", "value", 60)
			}
			if name == "worker-stopped" {
				stopWorker(cache.state)
				select {
				case <-cache.state.workerDone:
				case <-time.After(5 * time.Second):
					test.Fatal("worker did not stop")
				}
			}
			for iteration := 0; iteration < 3; iteration++ {
				cache.Close()
				assertClosedCache(test, cache)
			}
		})
	}
}

func TestCloseWaitsForWorkerWithCleanupPending(test *testing.T) {
	cache, _ := newTestCache(600, DefaultConfig())
	cache.Set("key", "value", 1)
	clockEntered := make(chan struct{})
	releaseClock := make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseClock) })
	cache.state.clock = func() int64 {
		close(clockEntered)
		<-releaseClock
		return 720
	}
	ticks := make(chan time.Time, 1)
	ticks <- time.Time{}
	startTestWorker(test, cache.state, ticks)
	test.Cleanup(release)

	select {
	case <-clockEntered:
	case <-time.After(5 * time.Second):
		test.Fatal("worker did not sample the clock")
	}

	const closeCount = 8
	returned := make(chan struct{}, closeCount)
	for caller := 0; caller < closeCount; caller++ {
		go func() {
			cache.Close()
			returned <- struct{}{}
		}()
	}
	select {
	case <-cache.state.stop:
	case <-time.After(5 * time.Second):
		test.Fatal("Close did not signal worker shutdown")
	}
	select {
	case <-returned:
		test.Fatal("Close returned while the worker was still running")
	case <-time.After(25 * time.Millisecond):
	}
	assertClosedCache(test, cache)

	release()
	for caller := 0; caller < closeCount; caller++ {
		select {
		case <-returned:
		case <-time.After(5 * time.Second):
			test.Fatal("Close did not return after the worker stopped")
		}
	}
	assertClosedCache(test, cache)
}

func TestCloseConcurrentWithOperations(test *testing.T) {
	cache := New(DefaultConfig())
	test.Cleanup(cache.Close)
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			key := strconv.Itoa(worker)
			for iteration := 0; iteration < 100; iteration++ {
				cache.Set(key, iteration, 60)
				cache.Get(key)
				cache.GetWithTTL(key)
				cache.Touch(key, 120)
				cache.Delete(key)
				cache.Stats()
				if iteration%11 == 0 {
					cache.Clear()
				}
				if iteration == 50 && worker%4 == 0 {
					cache.Close()
				}
			}
		}()
	}
	close(start)
	finished := make(chan struct{})
	go func() {
		waitGroup.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		test.Fatal("concurrent operations did not finish")
	}
	assertClosedCache(test, cache)
}

func TestRepeatedConcurrentClearAndClose(test *testing.T) {
	for _, scenario := range []struct {
		name      string
		callClear bool
		callClose bool
	}{
		{name: "clear-only", callClear: true},
		{name: "close-only", callClose: true},
		{name: "clear-and-close", callClear: true, callClose: true},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			cache := New(DefaultConfig())
			test.Cleanup(cache.Close)
			for index := 0; index < 32; index++ {
				cache.Set(strconv.Itoa(index), index, 60)
			}

			start := make(chan struct{})
			var waitGroup sync.WaitGroup
			for worker := 0; worker < 32; worker++ {
				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()
					<-start
					for iteration := 0; iteration < 100; iteration++ {
						if scenario.callClose && (!scenario.callClear || worker%2 == 0) {
							cache.Close()
							select {
							case <-cache.state.workerDone:
							default:
								test.Error("Close returned before the worker stopped")
								return
							}
						} else {
							cache.Clear()
						}
					}
				}()
			}
			close(start)
			finished := make(chan struct{})
			go func() {
				waitGroup.Wait()
				close(finished)
			}()
			select {
			case <-finished:
			case <-time.After(5 * time.Second):
				test.Fatal("repeated concurrent lifecycle calls did not finish")
			}

			if scenario.callClose {
				assertClosedCache(test, cache)
				return
			}
			if stats := cache.Stats(); stats.Total != 0 || len(stats.ByType) != 0 {
				test.Fatalf("Stats after repeated Clear = %+v, want empty", stats)
			}
			select {
			case <-cache.state.stop:
				test.Fatal("Clear signaled worker shutdown")
			default:
			}
			cache.Set("after-clear", "value", 60)
			if value, found := cache.Get("after-clear"); value != "value" || !found {
				test.Fatalf("Get after repeated Clear and Set = (%v, %v), want (value, true)", value, found)
			}
			assertIndexInvariant(test, cache)
		})
	}
}

func assertClosedCache(test *testing.T, cache *Cache) {
	test.Helper()

	cache.Clear()
	for _, key := range []string{"key", "nil", "self", "new-key", ""} {
		for _, ttlSeconds := range []int64{-1, 0, 1, DefaultMaxTTLSeconds + 1} {
			cache.Set(key, "new-value", ttlSeconds)
		}
		if value, found := cache.Get(key); value != nil || found {
			test.Fatalf("Get(%q) after Close = %v, %v, want nil, false", key, value, found)
		}
		if value, expiresAtUnix, ttlSeconds, found := cache.GetWithTTL(key); value != nil || expiresAtUnix != 0 || ttlSeconds != 0 || found {
			test.Fatalf("GetWithTTL(%q) after Close = (%v, %d, %d, %v), want miss", key, value, expiresAtUnix, ttlSeconds, found)
		}
		for _, ttlSeconds := range []int64{-1, 0, 60} {
			if cache.Touch(key, ttlSeconds) {
				test.Fatalf("Touch(%q, %d) after Close = true, want false", key, ttlSeconds)
			}
		}
		if cache.Delete(key) {
			test.Fatalf("Delete(%q) after Close = true, want false", key)
		}
	}
	if stats := cache.Stats(); stats.Total != 0 || len(stats.ByType) != 0 {
		test.Fatalf("Stats after Close = %+v, want empty", stats)
	}

	cache.state.mu.RLock()
	defer cache.state.mu.RUnlock()
	if !cache.state.closed {
		test.Fatal("cache was not marked closed")
	}
	if cache.state.entries != nil || cache.state.minuteBuckets != nil || cache.state.typeCounts != nil {
		test.Fatal("Close retained entry or index maps")
	}
}
