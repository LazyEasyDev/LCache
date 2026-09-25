package LCache

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestGlobalCacheBeforeInit(test *testing.T) {
	Close()
	test.Cleanup(Close)

	Set("key", "value", 60)
	if value, found := Get("key"); value != nil || found {
		test.Fatalf("Get before Init = (%v, %v), want (nil, false)", value, found)
	}
	if value, expiresAtUnix, ttlSeconds, found := GetWithTTL("key"); value != nil || expiresAtUnix != 0 || ttlSeconds != 0 || found {
		test.Fatalf("GetWithTTL before Init = (%v, %d, %d, %v), want miss", value, expiresAtUnix, ttlSeconds, found)
	}
	if Touch("key", 60) {
		test.Fatal("Touch before Init = true, want false")
	}
	if Delete("key") {
		test.Fatal("Delete before Init = true, want false")
	}
	Clear()
	if stats := GlobalStats(); stats.Total != 0 || len(stats.ByType) != 0 {
		test.Fatalf("GlobalStats before Init = %+v, want empty", stats)
	}
	Close()
}

func TestInitProvidesGlobalOperations(test *testing.T) {
	Close()
	test.Cleanup(Close)

	instance := Init(Config{MaxTTLSeconds: 60})
	Set("key", "value", 30)

	if value, found := Get("key"); value != "value" || !found {
		test.Fatalf("Get after Init = (%v, %v), want (value, true)", value, found)
	}
	if value, found := instance.Get("key"); value != "value" || !found {
		test.Fatalf("initialized Cache Get = (%v, %v), want (value, true)", value, found)
	}
	if value, expiresAtUnix, ttlSeconds, found := GetWithTTL("key"); value != "value" || expiresAtUnix == 0 || ttlSeconds < 0 || !found {
		test.Fatalf("GetWithTTL after Init = (%v, %d, %d, %v), want live value", value, expiresAtUnix, ttlSeconds, found)
	}
	if !Touch("key", 45) {
		test.Fatal("Touch after Init = false, want true")
	}
	if stats := GlobalStats(); stats.Total != 1 {
		test.Fatalf("GlobalStats total = %d, want 1", stats.Total)
	}
	if !Delete("key") {
		test.Fatal("Delete after Init = false, want true")
	}
	Set("clear", 1, 30)
	Clear()
	if stats := GlobalStats(); stats.Total != 0 {
		test.Fatalf("GlobalStats total after Clear = %d, want 0", stats.Total)
	}
}

func TestInitReusesGlobalCache(test *testing.T) {
	Close()
	test.Cleanup(Close)

	previous := Init(Config{MaxTTLSeconds: 30})
	Set("old", "value", 60)
	current := Init(DefaultConfig())

	if previous != current {
		test.Fatal("Init did not return the existing Cache")
	}
	if current.state.maxTTLSeconds != 30 {
		test.Fatalf("max TTL after repeated Init = %d, want 30", current.state.maxTTLSeconds)
	}
	select {
	case <-previous.state.workerDone:
		test.Fatal("Init stopped the existing Cache worker")
	default:
	}
	if value, found := Get("old"); value != "value" || !found {
		test.Fatalf("Get old key after repeated Init = (%v, %v), want (value, true)", value, found)
	}

	Set("new", "value", 60)
	if value, found := current.Get("new"); value != "value" || !found {
		test.Fatalf("existing Cache Get = (%v, %v), want (value, true)", value, found)
	}
}

func TestGlobalCacheCanReinitializeAfterClose(test *testing.T) {
	Close()
	test.Cleanup(Close)

	previous := Init(DefaultConfig())
	Close()
	assertClosedCache(test, previous)

	current := Init(DefaultConfig())
	if current == previous {
		test.Fatal("Init after Close returned the closed Cache")
	}
	Set("key", "value", 60)
	if value, found := Get("key"); value != "value" || !found {
		test.Fatalf("Get after reinitialization = (%v, %v), want (value, true)", value, found)
	}
}

func TestGlobalCacheConcurrentOperations(test *testing.T) {
	Close()
	test.Cleanup(Close)
	instance := Init(DefaultConfig())

	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			key := strconv.Itoa(worker)
			for iteration := 0; iteration < 100; iteration++ {
				Set(key, iteration, 60)
				Get(key)
				GetWithTTL(key)
				Touch(key, 60)
				GlobalStats()
				Delete(key)
				if iteration%11 == 0 {
					Clear()
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
		test.Fatal("concurrent global cache operations did not finish")
	}

	Set("final", "value", 60)
	if value, found := Get("final"); value != "value" || !found {
		test.Fatalf("Get after concurrent operations = (%v, %v), want (value, true)", value, found)
	}
	Close()
	assertClosedCache(test, instance)
}
