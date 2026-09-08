# Cache Redesign

Status: implemented baseline. The previous implementation has been removed and does not constrain the replacement.

Target Go version: 1.25.13.

## Purpose

Provide a process-local L1 cache for application values, typically in front of Redis. Expiration uses second-based TTLs, exact Unix-second deadlines, and a cached clock refreshed about once per second. Sparse absolute-minute buckets handle physical cleanup without one timer or goroutine per entry.

## Goals

- Average O(1) lookup and write work.
- Fast expiration validation from a cached current time.
- Expected O(1) expiration scheduling and same-key replacement with sparse absolute-minute buckets.
- Exactly one minute-bucket record for each resident entry.
- Safe TTL replacement and extension using immutable entry identity.
- Bounded expiration-cleanup work per write-lock acquisition without an explicit shutdown lifecycle.
- Automatic worker termination and reclamation after the cache becomes unreachable.
- Atomic removal of all cached values through `Clear`.
- Exact resident-entry counts grouped by concrete Go type.
- Conventional `Cache` methods for heterogeneous values.

## Non-goals

- Exact measurement of Go heap ownership.
- Automatic memory or entry-capacity eviction.
- Cloning values or enforcing ownership and immutability.
- LRU or LFU admission and eviction in the first implementation.
- Distributed invalidation or coherence with Redis.
- Arbitrary score/rank queries over expiration records.
- Exact-second physical removal; logical expiration remains exact to the cached Unix second.
- Automatic shrinking of Go map backing storage after high-water usage.
- Preserving expiration monotonicity across backward system wall-clock adjustments.

## Public API

```go
package cache

import (
	"reflect"
	"time"
)

const DefaultMaxTTLSeconds int64 = 24 * 60 * 60

const clockUpdateInterval = time.Second
const workerBatchSize = 4 * 1024
const cleanupGraceMinutes int64 = 5

type Config struct {
	MaxTTLSeconds int64
}

func DefaultConfig() Config

type Stats struct {
	Total  int
	ByType map[reflect.Type]int
}

type Cache struct {
	noCopy noCopy
	state *cacheState
}

func New(config Config) *Cache
func (c *Cache) Set(key string, value any, ttlSeconds int64)
func (c *Cache) Get(key string) (value any, expiresAtUnix int64, found bool)
func (c *Cache) Touch(key string, ttlSeconds int64) (found bool)
func (c *Cache) Delete(key string) (found bool)
func (c *Cache) Clear()
func (c *Cache) Stats() Stats
```

`Cache` must not be copied after construction. Copying a `*Cache` pointer is supported and is the normal way to share one cache:

```go
alias := local
```

Dereferencing and copying the struct, as in `copied := *local`, is unsupported because cleanup is registered on the original wrapper. The implementation includes the conventional `noCopy` marker so `go vet` reports accidental struct copies. Go does not otherwise prohibit copying an exported struct at compile time.

`DefaultConfig` returns a fresh configuration with `MaxTTLSeconds` set to `DefaultMaxTTLSeconds`, which is 24 hours. `New` always succeeds. A configured maximum is valid only when it is between one second and `DefaultMaxTTLSeconds`, inclusive. Zero, negative, and larger values are invalid and fall back to `DefaultMaxTTLSeconds`. This keeps the constructor infallible and prevents an unreasonable configured TTL from reaching deadline arithmetic.

One `Cache` can hold values of different types. `Set` accepts `any`, and `Get` returns `any`; callers use a type assertion when they need a concrete type:

```go
config := cache.DefaultConfig()
local := cache.New(config)

local.Set("name", "Alice", 60)
local.Set("person:1", Person{Name: "Alice"}, 60)

value, expiresAtUnix, found := local.Get("name")
name, typeMatched := value.(string)

personValue, personExpiresAtUnix, personFound := local.Get("person:1")
person, personTypeMatched := personValue.(Person)
```

The `found` result reports cache presence and expiration only. A caller-side type assertion separately reports whether the stored dynamic type matches the requested application type. Keeping `Set` and `Get` as methods makes the API conventional and avoids package-level generic operations.

The cache stores each value exactly as supplied, including bare nil and typed nil values, and does not clone it. Value types such as strings are copied into the cache entry. Pointers, maps, slices, and other reference-bearing values continue to reference caller-visible data, so callers remain responsible for synchronization and mutation policy.

`Get` succeeds only when the key exists and has not expired. It returns the stored value unchanged as `any` and the entry's absolute expiration as Unix seconds. Type matching is not part of cache lookup:

```go
local.Set("name", "Alice", 60)

value, _, found := local.Get("name")
if found {
	name, ok := value.(string)
	if !ok {
		panic("unexpected stored type")
	}
	_ = name
}
```

`Set` returns nothing. A positive-TTL call stores every value, including bare nil interfaces and typed nil pointers, maps, slices, channels, functions, and interfaces. A non-positive TTL is a no-op and does not change the map, minute buckets, or statistics. TTLs above `MaxTTLSeconds` are clamped. `Touch` is the explicit operation for changing an existing value's TTL.

A stored bare nil is distinguishable from a miss through `found`:

```go
local.Set("optional", nil, 60)

value, expiresAtUnix, found := local.Get("optional")
// value == nil, expiresAtUnix > 0, found == true
```

`Set`, positive `Touch`, `Delete`, and `Clear` update the authoritative entry map and minute buckets together under the cache write lock. They never leave a resident entry waiting for background scheduling. Each operation performs a constant expected number of Go map operations.

The empty string is a valid cache key and is treated like any other key. Public methods require a non-nil `*Cache` returned by `New`; calling a method on a nil `*Cache` is a caller programming error and may panic naturally. Methods do not add defensive nil-receiver checks.

The booleans returned by `Touch` and `Delete` report only whether a live entry existed at the operation's linearization point. An expired entry is logically nonexistent even if physical cleanup has not removed it yet.

An entry is expired exactly when:

```go
entry.expiresAtUnix <= cachedNowUnix
```

Equality is expired, not live. `Get` therefore returns a miss at the deadline, `Touch` returns `false`, and `Delete` physically removes the resident record but returns `false`.

`Touch` returns `false` only when the key does not identify a live entry according to the cached clock. For a live entry it returns `true`; a positive TTL updates the expiration, a TTL above `MaxTTLSeconds` is clamped, and a non-positive TTL leaves the expiration unchanged.

`Delete` returns `false` only when the key does not identify a live entry. Otherwise it removes the entry and returns `true`. An expired entry may still be removed physically during the call, but it returns `false` because it is logically nonexistent.

`Clear` atomically replaces the entry map, minute-bucket map, and type-count map with new empty structures under the write lock; it does not iterate over individual records. Because no old bucket remains, it also resets `cleanedMinute` to `state.clock()/60 - cleanupGraceMinutes` rather than walking elapsed empty minutes. Calls that begin after `Clear` returns observe an empty cache with zeroed statistics, and the cache remains reusable. After the lock is released, the old structures are unreachable and the Go runtime may reclaim them normally. `Clear` must not call `runtime.GC`.

A concurrent operation is ordered either before or after the structure swap by the cache mutex. `Clear` is only a user-facing record operation: it does not close `state.stop`, stop the maintenance worker, or otherwise change the cache lifecycle.

## Core Data Model

```go
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
	mu             sync.RWMutex
	entries        map[string]*entry
	minuteBuckets  map[int64]*minuteBucket
	cleanedMinute  int64
	typeCounts     map[reflect.Type]int
	cachedNowUnix atomic.Int64
	clock         func() int64
	stop          chan struct{}
	stopOnce      sync.Once
	workerDone    chan struct{}
	// Configuration and statistics follow.
}

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

type Cache struct {
	noCopy noCopy
	state *cacheState
}
```

The public `Cache` is a small wrapper. All mutable data lives in `cacheState`. The `entries` map is authoritative, and `minuteBuckets` is its expiration index. Every positive `Set` and every successful positive `Touch` allocates a new `entry`. Its cache metadata is immutable after publication, so pointer identity distinguishes a current entry from an older same-key entry without a numeric version. The stored value itself retains the caller-visible mutation semantics described above.

`minuteBuckets` contains only nonempty buckets. Each bucket stores its absolute Unix minute and is also keyed by that minute in the outer map. Inner maps are created lazily on first insertion and removed when empty. The maximum TTL bounds the ordinary future scheduling horizon, while absolute minute keys avoid circular-slot reuse, range clamping, and overflow handling.

While the mutex is held at every operation boundary, each pointer in `entries` appears under the same key in exactly one bucket selected by `entry.expiresAtUnix/60`. Replacing or deleting a key removes its old bucket record before publishing the final state. `New` initializes both maps and sets `cleanedMinute` to `initialUnixMinute - cleanupGraceMinutes`; no earlier bucket can exist in a new cache. As a defensive measure for a backward clock adjustment, inserting into a minute at or before `cleanedMinute` rewinds the cursor to one minute before that bucket so the record cannot be permanently stranded.

## Read Path

`Get` performs these operations under a read lock:

1. Look up the key in `entries`.
2. Atomically load the cached current time.
3. Treat the entry as expired when `expiresAtUnix <= cachedNowUnix`.
4. Return a miss if the entry is absent or expired.
5. Return the stored `any` value and `expiresAtUnix` with `found == true`.

A miss returns `nil`, a zero expiration timestamp, and `found == false`. The boolean distinguishes a stored nil or zero value, such as an empty string or zero integer, from a miss. Returning the absolute Unix-second deadline avoids remaining-TTL arithmetic in the read path. A caller that needs a remaining TTL can subtract its own current Unix time.

`New` initializes the cached time before returning the cache. The worker refreshes it on every tick. Reads therefore avoid calling `time.Now`, but expiration accuracy follows the worker's clock refresh rather than the wall clock directly. A value can remain visible briefly after its wall-clock deadline. The expected lag is about one second, but a scheduler or process pause can extend it; there is no hard upper bound.

Store the cached Unix second through `atomic.Int64` so refreshes do not race with readers. Keep the production clock as an internal function returning `time.Now().Unix()`, allowing tests to inject controlled Unix seconds. `Set` and a positive `Touch` sample that clock directly when establishing a new deadline, avoiding a shortened TTL when the cached read clock is old.

Expiration intentionally follows the system wall clock. A rare backward clock adjustment may temporarily extend an entry or make an expired-but-not-yet-removed entry appear live again. The cache does not clamp the cached Unix second or provide monotonic expiration across clock corrections.

Because `time.Now().Unix()` truncates the fractional second, deadline creation adds one extra second:

```go
expiresAtUnix = saturatingAdd(nowUnix, ttlSeconds, 1)
```

This guarantees that an entry remains live for at least the requested number of whole seconds under a normally advancing wall clock. Its deadline may be almost one second later than the exact requested duration, in addition to the cached-clock visibility delay. `Set` and positive `Touch` must use the same overflow-safe calculation.

## Write Paths

`Set` performs one atomic entry-and-bucket transition under the write lock:

1. Return before locking only when `ttlSeconds` is non-positive; nil values are valid.
2. Clamp `ttlSeconds` to `MaxTTLSeconds` when necessary.
3. Acquire the write lock, sample the current Unix second, and add `ttlSeconds + 1` to establish `expiresAtUnix`. Saturate at `math.MaxInt64` if either addition would overflow; this is defensive for injected or abnormal clocks because configured TTLs are capped at 24 hours.
4. If the key already exists, remove it from the bucket selected by the old entry's deadline and remove that bucket from `minuteBuckets` if it becomes empty.
5. Allocate the new immutable entry, store it in `entries`, and store the same pointer under the key in the lazily allocated bucket selected by the new deadline.
6. Update the old and new concrete-type counts and release the write lock.

`Touch` follows the same direct bucket-update path:

1. Under the write lock, look up the key and check it against the cached Unix second.
2. Return `false` when no live entry exists.
3. For a live entry with a non-positive `ttlSeconds`, release the lock and return `true` without changing anything.
4. Clamp a positive `ttlSeconds`, sample the current Unix second, and calculate `expiresAtUnix` exactly as `Set` does.
5. Remove the key from its old minute bucket, deleting that bucket if it becomes empty.
6. Replace the authoritative pointer with a new immutable `entry` carrying the same value and type and the new deadline, insert that pointer into the new minute bucket, release the lock, and return `true`.

`Delete` looks up the entry and determines its live return status under the write lock. When the key is physically present, it removes the authoritative entry, removes the matching minute-bucket record and any newly empty bucket, updates the type count, and releases the lock. It returns `true` only when the removed entry was live; an absent key returns `false` without changing either map.

## Absolute Minute Buckets

Physical expiration uses the absolute Unix minute containing the exact deadline:

```go
dueMinute := expiresAtUnix / 60
```

The worker processes only minutes at or before:

```go
safeMinute := currentUnixMinute - cleanupGraceMinutes
```

Therefore, every second in a selected bucket is already expired before physical removal begins. Depending on the deadline's second within its minute, normal physical removal starts roughly four to five minutes after the exact deadline, plus cached-clock, batching, and scheduling delay. `Get`, `Touch`, and `Delete` continue to use the exact Unix-second deadline and do not inherit minute-level logical expiration.

The outer map accepts any absolute Unix minute, so a requested bucket never needs to be clamped or redirected. Only occupied minutes allocate inner maps. Under normal cleanup, the 24-hour maximum TTL bounds future occupied minutes and the five-minute grace bounds retained past minutes. There is no fixed-size circular wheel and no overflow bucket.

Removing an old placement and inserting its replacement are expected O(1) map operations. There is no queue, scheduled-placement map, heap, heap index, stale-record accumulation, or compaction pass.

The worker cleans minutes in ascending order from `cleanedMinute + 1` through `safeMinute`. It processes at most `workerBatchSize` records or empty minute advances in one lock hold. Each selected record is removed from its source bucket before validation so every batch makes progress. If `entries[key]` still contains the same entry pointer, cleanup removes the authoritative entry and updates its type count. A newer authoritative pointer is left untouched. No second-level deadline check is needed: every deadline in a bucket at or before `safeMinute` is necessarily expired. When the current bucket is empty, the worker removes it and advances `cleanedMinute`. It never advances past a nonempty partially processed bucket.

If the clock or worker jumps forward across many empty minutes, bounded empty-minute advancement continues over multiple worker iterations. This preserves bounded lock holds; a later implementation may skip directly to the next occupied minute if profiling shows catch-up latency matters.

## Maintenance Worker

`New` creates a separate `cacheState`, starts exactly one expiration worker with that state, and registers a cleanup on the public `Cache` wrapper:

```go
func stopWorker(state *cacheState) {
	close(state.stop)
}

func New(config Config) *Cache {
	state := newCacheState(config)
	c := &Cache{state: state}

	runtime.AddCleanup(c, stopWorker, state)
	go state.runWorker()

	return c
}
```

The worker captures only `*cacheState`; it must never capture or store `*Cache`. Go's garbage collector treats a reference held by a goroutine like any other strong reference; it cannot distinguish an internal worker reference from an application reference. If the worker captured `*Cache`, the wrapper would remain reachable forever and its cleanup would never run.

The wrapper therefore creates the required ownership boundary: application code references `*Cache`, while the worker references only `*cacheState`. Likewise, `cacheState`, `stopWorker`, and the cleanup argument must not contain a path back to the `Cache` wrapper.

The worker uses one ticker with the fixed `clockUpdateInterval` plus minute-cleanup pending state:

1. At the start of each iteration, handle a ready `state.stop` or ticker event first. Wait for a ticker event or stop signal only when no cleanup work is immediately available.
2. On every tick, sample `state.clock` and atomically publish the new cached time.
3. Calculate `safeMinute := nowUnix/60 - cleanupGraceMinutes`. Mark minute cleanup pending whenever `cleanedMinute < safeMinute`. A later tick may advance the safe minute while cleanup remains pending.
4. When minute cleanup is pending, process at most `workerBatchSize` records or empty minute advances under one write-lock section. Keep it pending while `cleanedMinute < safeMinute` or the next eligible bucket remains partially processed.
5. Each active iteration performs at most one minute-cleanup batch. It checks stop and ticker events between iterations so no cleanup backlog starves clock refresh or shutdown.
6. Call `runtime.Gosched` between active iterations and continue until no immediate work remains, then wait for the next event.
7. Stop the ticker and return when `state.stop` is closed.

The one-second clock refresh only performs a clock sample and atomic integer store; it does not acquire the cache write lock. Minute cleanup processes at most `workerBatchSize` records or empty minute advances per write-lock acquisition; no wall-clock duration bound is promised. Releasing the lock between batches allows waiting cache operations to proceed.

`Clear` replaces the authoritative and derived indexes under the write lock. Worker batches must read current state fields only while holding that lock and retain no bucket or entry pointers across an unlock; this allows `Clear` to make the replaced structures unreachable immediately.

Under normal scheduling, physical cleanup starts when the deadline's absolute minute is five minutes behind the current absolute minute. This is roughly four to five minutes after the exact expiration, plus cached-clock and scheduling delay. A large bucket or worker backlog may require multiple batches. Logical expiration observed by `Get`, `Touch`, and `Delete` follows the cached clock and normally lags wall-clock expiration by less than one clock-update interval. Runtime scheduling and process suspension can increase all delays.

When no ordinary reachable reference to the `Cache` wrapper remains, a future garbage collection may queue `stopWorker(state)`. Closing `state.stop` wakes the worker and makes it return. The cleanup does not call `Clear` or manually empty the state: after the cleanup callback and worker release their references, the wrapper, state, entries, minute buckets, stop channel, ticker, and cached values are unreachable and eligible for garbage collection together.

Every public method first obtains `state := c.state` and immediately schedules `defer runtime.KeepAlive(c)`. This keeps the wrapper alive across every return path so its cleanup cannot stop maintenance during an active operation. Cleanup timing is nondeterministic and cleanup is not guaranteed to run before process exit; this mechanism provides automatic eventual reclamation, not synchronous shutdown.

## Count Statistics

The cache does not estimate object-graph memory and does not enforce a record limit. Applications know their value shapes and resource budgets better than this package. The cache instead exposes exact resident-record statistics so applications can monitor usage and choose their own policy.

`Stats.Total` is the total number of entries physically resident in the map. `Stats.ByType` maps each value's concrete dynamic `reflect.Type` to its resident count. A bare nil has no dynamic type and is counted under the valid nil `reflect.Type` map key. Minute-bucket records are internal bookkeeping and are never counted separately. `Stats` returns a newly allocated map, so callers may read or modify it without affecting cache state. Because read correctness does not wait for cleanup, these statistics may include entries that expired during the cleanup grace period or a maintenance backlog and already produce cache misses.

```text
Total: 120
ByType:
  *example.User: 100
  *example.Order: 20
```

`Set` obtains the value's concrete dynamic type with `reflect.TypeOf(value)`, which returns nil for a bare nil value and the concrete type for a typed nil value. A new key increments its type count. Replacing a key with a different concrete type decrements the old count and increments the new count. Deletion and physical expiration decrement the stored entry type; a zero count removes that type from `typeCounts`.

All entry and type-count updates occur in the same write-locked transaction. This keeps the statistics exact without a second per-entry index or a background scan. The cost is one `reflect.TypeOf` call and constant-time counter updates per write. Logical index size is proportional to resident entries and occupied minutes, but Go maps do not automatically shrink their backing storage after heavy churn. `Clear` replaces the maps and allows their previous backing storage to be reclaimed; no automatic high-water compaction runs during ordinary operation.

## Concurrency Model

- All authoritative map, minute-bucket, and type-count mutations occur under one mutex initially.
- `Get` uses the corresponding read lock.
- The worker publishes the cached Unix second through `atomic.Int64`; readers perform one atomic load per operation.
- `Stats` copies `Total` and `ByType` under the read lock, then returns the independent snapshot.
- The worker performs ordinary expiration-index mutations; `Clear` may replace the entire index under the same mutex.
- No package-global mutable configuration is used.

A single lock is the baseline because authoritative entries, expiration buckets, and accounting form one transaction. Immutable entry metadata and pointer comparison prevent cleanup from deleting a newer same-key value. Sharding should be considered only after representative benchmarks identify lock contention.

## Failure and Edge Cases

- Bare nil and typed nil values are stored when their TTL is positive.
- Non-positive TTLs make `Set` a no-op regardless of value.
- Empty string keys are valid.
- Calling any method on a nil `*Cache` is a caller error and may panic naturally.
- A configured `MaxTTLSeconds` outside `1..DefaultMaxTTLSeconds` falls back to `DefaultMaxTTLSeconds`.
- Calling `Get` on an entry expired according to the cached clock returns a miss immediately. It does not trigger physical cleanup.
- `Get` does not perform type matching; callers type-assert the returned value when needed.
- `Touch` and `Delete` return `false` only when no live entry exists for the key.
- `Clear` removes live and expired entries, resets statistics, and leaves the cache reusable.
- An unreachable `Cache` wrapper eventually signals its worker to exit through `runtime.AddCleanup`; the worker state has no reference back to the wrapper.
- Replacing or deleting a key updates the authoritative map and its minute-bucket record atomically under the write lock.
- Physical cleanup deliberately retains expired entries until their deadline minute is at least five minutes behind the current minute.
- Go map backing storage may retain a historical high-water allocation until `Clear` or cache reclamation.
- Concurrent mutation of reference-bearing cached values is the caller's responsibility and may create data races.
- Backward system-clock adjustments may extend, revive, or prematurely remove physically resident entries; monotonic expiration is outside the cache's guarantees. Cursor rewind still ensures newly inserted records remain eligible for eventual physical cleanup after the clock recovers.

## Validation Plan

- Unit-test second-based expiration with an injected fake clock; do not use multi-second sleeps.
- Test replacement, TTL extension, deletion/reinsertion, and direct old-bucket removal. Construct an internal stale bucket record to verify that cleanup removes that source record without deleting or unindexing a newer authoritative pointer.
- Test deadline-to-minute flooring at exact boundaries and one second on either side.
- Test sparse bucket allocation, empty-bucket removal, the five-minute cleanup cutoff, cleanup cursor advancement, and same-key movement between absolute-minute buckets.
- Test heterogeneous values in one cache and caller type assertions for matching and mismatched stored types.
- Test the 24-hour default, valid custom `MaxTTLSeconds` values, and fallback from zero, negative, and above-default values.
- Test saturating `nowUnix + ttlSeconds + 1` deadline arithmetic with an injected clock near `math.MaxInt64`.
- Test storage, retrieval, replacement, deletion, expiration, and type counts for bare nil and each typed nil reference kind.
- Test non-positive-TTL no-op behavior with both nil and non-nil values.
- Test empty string keys and the documented nil-receiver panic behavior.
- Test `Touch` and `Delete` results for live, absent, and expired entries; verify that a non-positive `Touch` leaves a live entry unchanged and returns `true`.
- Test the exact deadline boundary: equality is a `Get` miss, `Touch` returns `false`, and `Delete` removes the resident entry but returns `false`.
- Test `Clear` against live and expired entries, an already-empty cache, concurrent operations, and cache reuse.
- Test cached-clock initialization, atomic refresh, one-second expiration granularity, the extra deadline second for `Set` and `Touch`, and returned Unix expiration timestamps.
- Test minute cleanup independently of wall-clock sleeps. Extract clock refresh and due-bucket cleanup for deterministic invocation.
- Test bucket cleanup and empty-minute advancement below, at, and above `workerBatchSize`; verify that partial buckets prevent cursor advancement and that every batch removes distinct source records.
- Verify that each maintenance function performs only one batch per call, reports remaining work, and permits ticker, stop, and other work before its next batch.
- Verify after every `Set`, positive `Touch`, and removing `Delete` that the authoritative map and minute buckets contain exactly one matching record or neither contains the key.
- Race `Clear` with writes and minute cleanup; verify that old buckets become unreachable and current entries remain indexed. Also call `Clear` during empty-minute and partial-bucket backlogs, insert a new entry, and verify that the reset cursor cannot consume or skip its bucket.
- Test with forced garbage collections that an unreachable cache queues cleanup and its worker exits; use bounded synchronization rather than relying on final process shutdown.
- Test that public operations keep the cache wrapper alive until each operation completes.
- Run `go vet` to detect accidental `Cache` struct copies; verify that pointer aliases remain supported.
- Test exact type counts for insertion, same-type replacement, cross-type replacement, deletion, and expiration.
- Run all tests with `go test -race`.
- Benchmark `Get`, new-key `Set`, overwrite, `Touch`, `Delete`, minute cleanup, and statistics snapshots independently.
- Benchmark complete write transactions, including direct bucket maintenance.

## Implementation Order

1. Implement heterogeneous map entries and second-based method reads using an injectable clock and atomic cached time.
2. Add immutable entry replacement, direct sparse absolute-minute bucket maintenance, and deterministic expiration tests.
3. Add bounded minute cleanup and exact count statistics.
4. Add the worker, `runtime.AddCleanup` lifecycle, `runtime.KeepAlive` calls, and `Clear`.
5. Run race tests and representative concurrent benchmarks before tuning or sharding.