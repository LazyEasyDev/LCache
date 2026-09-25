package LCache

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func startTestWorker(test *testing.T, state *cacheState, ticks <-chan time.Time) {
	test.Helper()
	go func() {
		defer close(state.workerDone)
		state.runWorkerWithTicks(ticks)
	}()
	test.Cleanup(func() {
		stopWorker(state)
		select {
		case <-state.workerDone:
		case <-time.After(5 * time.Second):
			test.Error("worker did not stop")
		}
	})
}

func TestWorkerRefreshesClockBetweenCleanupBatches(test *testing.T) {
	cache, _ := newTestCache(600, DefaultConfig())
	for index := 0; index < workerBatchSize+1; index++ {
		cache.Set(strconv.Itoa(index), index, 1)
	}
	cache.Set("next-minute", "value", 60)

	var clockReads atomic.Int64
	cache.state.clock = func() int64 {
		if clockReads.Add(1) == 1 {
			return 660
		}
		return 720
	}
	ticks := make(chan time.Time, 2)
	ticks <- time.Time{}
	ticks <- time.Time{}
	startTestWorker(test, cache.state, ticks)

	deadline := time.Now().Add(5 * time.Second)
	for cache.state.cachedNowUnix.Load() != 720 || cache.Stats().Total != 0 {
		if time.Now().After(deadline) {
			test.Fatal("worker did not refresh the clock and drain both due minutes")
		}
		runtime.Gosched()
	}
	if got := clockReads.Load(); got != 2 {
		test.Fatalf("clock reads = %d, want 2", got)
	}
	assertIndexInvariant(test, cache)
}

func TestWorkerStopsWithCleanupPending(test *testing.T) {
	cache, _ := newTestCache(600, DefaultConfig())
	initialMinute := cache.state.cleanedMinute
	targetMinute := initialMinute + 2*workerBatchSize
	clockEntered := make(chan struct{})
	releaseClock := make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseClock) })
	cache.state.clock = func() int64 {
		close(clockEntered)
		<-releaseClock
		return (targetMinute + cleanupGraceMinutes) * secondsPerMinute
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
	stopWorker(cache.state)
	release()
	select {
	case <-cache.state.workerDone:
	case <-time.After(5 * time.Second):
		test.Fatal("worker did not stop with cleanup pending")
	}

	if got, want := cache.state.cleanedMinute, initialMinute+workerBatchSize; got != want {
		test.Fatalf("cleaned minute = %d, want %d after one bounded batch", got, want)
	}
	assertIndexInvariant(test, cache)
}

func TestConcurrentWorkerStopIsIdempotent(test *testing.T) {
	cache, _ := newTestCache(600, DefaultConfig())
	startTestWorker(test, cache.state, nil)
	var waitGroup sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			stopWorker(cache.state)
		}()
	}
	waitGroup.Wait()

	select {
	case <-cache.state.workerDone:
	case <-time.After(5 * time.Second):
		test.Fatal("worker did not stop after concurrent stop requests")
	}
}
