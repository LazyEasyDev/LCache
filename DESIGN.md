# LCache Design

This document describes the implemented architecture and behavioral contracts
of LCache. The package is an in-process, concurrent L1 cache for heterogeneous
Go values with second-based TTLs and automatic background cleanup.

Target Go version: 1.25.13.

## Design Summary

LCache separates expiration into two concerns:

1. **Logical expiration** decides whether an operation can observe an entry.
   It compares an exact Unix-second deadline with a cached current time.
2. **Physical cleanup** reclaims expired records later. One worker scans sparse
   buckets keyed by absolute Unix minute and removes records in bounded batches.

The authoritative key map, expiration index, and type counters are updated
together under one mutex. Entry metadata is immutable after publication, so a
pointer comparison can distinguish a current entry from an obsolete record.

This design gives normal cache operations expected O(1) work, avoids a timer
or goroutine per key, and keeps cleanup lock holds bounded by a work count.

## Goals

- Concurrent `Get`, `Set`, `Touch`, `Delete`, `Clear`, and `Stats` operations.
- Expected O(1) lookup, mutation, and expiration-index maintenance.
- Exact second-based logical expiration using a low-cost cached clock.
- One expiration-index record for every physically resident entry.
- Direct removal of old expiration placements on replacement and deletion.
- Bounded cleanup work per write-lock acquisition.
- Exact resident counts grouped by concrete Go type.
- Automatic worker shutdown after the public cache becomes unreachable.
- Atomic, constant-time removal of all records with `Clear`.

## Non-goals

- Memory-size or entry-count capacity limits.
- LRU, LFU, admission, or other capacity-eviction policies.
- Deep copying, ownership enforcement, or synchronization of stored values.
- Distributed invalidation or coherence with another cache.
- Exact-second physical reclamation.
- Monotonic TTL behavior across wall-clock corrections.
- Automatic shrinking of Go map backing storage after high-water usage.
- Synchronous worker shutdown through a public `Close` method.

## Public Contract

The exported surface is:

```go
const DefaultMaxTTLSeconds int64 = 24 * 60 * 60

type Config struct {
	MaxTTLSeconds int64
}

func DefaultConfig() Config

type Stats struct {
	Total  int
	ByType map[reflect.Type]int
}

func New(config Config) *Cache
func (c *Cache) Set(key string, value any, ttlSeconds int64)
func (c *Cache) Get(key string) (value any, expiresAtUnix int64, found bool)
func (c *Cache) Touch(key string, ttlSeconds int64) bool
func (c *Cache) Delete(key string) bool
func (c *Cache) Clear()
func (c *Cache) Stats() Stats
```

`New` is infallible. It normalizes `MaxTTLSeconds` to the 24-hour default when
the configured value is outside the inclusive range `1..86400`.

### Operation Semantics

| Operation | Live key | Expired key | Missing key |
| --- | --- | --- | --- |
| `Get` | returns value, deadline, `true` | returns `nil, 0, false` | returns `nil, 0, false` |
| `Set`, positive TTL | replaces value and deadline | replaces value and deadline | inserts value and deadline |
| `Set`, non-positive TTL | no-op | no-op | no-op |
| `Touch`, positive TTL | replaces deadline, returns `true` | no-op, returns `false` | returns `false` |
| `Touch`, non-positive TTL | no-op, returns `true` | no-op, returns `false` | returns `false` |
| `Delete` | removes record, returns `true` | removes record, returns `false` | returns `false` |

`Clear` atomically replaces all record and index maps with empty maps. Calls
ordered after it by the cache mutex observe an empty cache. The cache remains
usable and the worker lifecycle is unchanged.

`Stats` returns a snapshot with a newly allocated `ByType` map. Mutating that
map cannot affect the cache. Counts include all physically resident records,
including logically expired records awaiting cleanup.

### Keys and Values

- The empty string is a valid key.
- Values are accepted as `any`, including bare and typed nil values.
- A `Get` result must be converted with a caller-side type assertion.
- The `found` boolean distinguishes a stored nil from a miss.
- Values are stored directly and are not cloned.
- Callers own synchronization for mutable pointers, maps, slices, and other
  reference-bearing values.
- Calling a method on a nil `*Cache` is a programming error and may panic.

### Copying

`Cache` must not be copied after construction. Pointer aliases are supported:

```go
alias := local
```

Copying the wrapper with `copied := *local` is unsupported because runtime
cleanup is registered on the original wrapper. A conventional `noCopy` field
allows `go vet` to detect common accidental copies.

## Data Model

The implementation centers on these structures:

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

type Cache struct {
	noCopy noCopy
	state  *cacheState
}
```

The `Cache` wrapper exists separately from `cacheState` to support automatic
worker cleanup. Application code owns the wrapper; the worker retains only the
state.

### Authoritative State

`entries` is the authoritative key-to-record map. `minuteBuckets` is a derived
expiration index, and `typeCounts` is derived accounting. All three are
protected by `mu`.

Every positive `Set` and successful positive `Touch` creates a new `entry`.
The value, concrete type, and deadline metadata are not changed after the
entry is published. Reference-bearing values may still point to mutable data;
metadata immutability does not imply deep value immutability.

### Invariants

At every public operation boundary while `mu` is held:

1. Every record in `entries` appears under the same key in exactly one minute
   bucket.
2. The selected bucket key equals `entry.expiresAtUnix / 60`.
3. Every bucket record points to the authoritative record for that key.
4. `minuteBuckets` contains no empty bucket.
5. `typeCounts[t]` equals the number of resident entries whose stored type is
   `t`; zero counts are absent.

A bare nil value has `reflect.TypeOf(value) == nil` and is counted under the
valid nil key in `map[reflect.Type]int`. A typed nil is counted under its
concrete type.

## Time and Expiration

### Exact Deadline

An entry is live precisely when:

```go
entry.expiresAtUnix > cachedNowUnix
```

Equality is expired. The deadline returned by `Get` is an absolute Unix-second
value, not a remaining TTL.

`time.Now().Unix()` truncates subsecond time. To ensure at least the requested
number of whole seconds under a normally advancing clock, `Set` and positive
`Touch` calculate:

```go
expiresAtUnix = nowUnix + ttlSeconds + 1
```

The calculation saturates at `math.MaxInt64`. TTL input is already capped at
24 hours, but saturation keeps the helper correct for an abnormal or injected
clock near the integer limit.

### Cached Read Clock

`New` initializes `cachedNowUnix` synchronously. The worker refreshes it from
`state.clock` about once per second using `atomic.Int64`.

`Get`, `Touch`, and `Delete` use this cached value to decide whether an entry
is live. They therefore avoid a system clock call, but can observe an entry as
live briefly after its wall-clock deadline. The normal lag is around one
second; scheduling delays and process suspension mean there is no hard upper
bound.

`Set` and positive `Touch` sample `state.clock` directly when creating a new
deadline. A stale cached read clock therefore cannot shorten a newly assigned
TTL.

Expiration follows the system wall clock. A backward adjustment can extend or
revive a physically resident entry. The design does not provide monotonic TTL
semantics.

## Mutation Transactions

### Set

For a positive TTL, `Set`:

1. Clamps the TTL to `maxTTLSeconds`.
2. Acquires `mu` for writing.
3. Samples the clock and creates a new immutable record.
4. Removes any previous record from its minute bucket and type count.
5. Publishes the new record in `entries`, its due-minute bucket, and its type
   count.

A non-positive TTL returns before locking and changes nothing, including when
the key already exists.

### Touch

`Touch` acquires the write lock and checks the existing record against the
cached clock. It returns `false` without mutation for an absent or expired
record. A live record with a non-positive requested TTL returns `true` without
mutation.

For a live record and positive TTL, it creates a new record carrying the same
value and type, removes the old bucket placement, replaces the authoritative
pointer, and inserts the new bucket placement. Type counts do not change.

### Delete

`Delete` acquires the write lock and records whether the resident entry is
live. If present, it removes the authoritative entry, its matching bucket
placement, and its type count. It returns the previously determined live
status. This is why deleting an expired but resident record returns `false`
while still reclaiming it.

### Clear

`Clear` swaps `entries`, `minuteBuckets`, and `typeCounts` for new empty maps
under the write lock. It also resets:

```go
cleanedMinute = currentUnixMinute - cleanupGraceMinutes
```

The operation does not iterate over old records, call `runtime.GC`, stop the
worker, or retain old bucket pointers after unlocking. Old structures become
eligible for garbage collection when no concurrent operation references them.

## Minute-Bucket Index

Each entry is placed in the bucket for the absolute Unix minute containing its
deadline:

```go
dueMinute := expiresAtUnix / 60
```

Buckets are sparse. The outer map and an inner key map allocate storage only
for occupied minutes. An empty bucket is removed immediately after replacement
or deletion. Absolute minute keys avoid circular-slot reuse, overflow buckets,
and range remapping.

The ordinary future horizon is bounded by the configured maximum TTL. The
index can retain expired records during the cleanup grace period or a worker
backlog.

### Cleanup Safety Boundary

On each tick, the worker computes:

```go
safeMinute := unixMinute(nowUnix) - cleanupGraceMinutes
```

`cleanupGraceMinutes` is 1. A bucket is eligible only when its minute is at or
before `safeMinute`. At that point every second in the bucket is in the past,
so cleanup does not need another deadline comparison.

An entry normally remains physically resident for up to about one minute
after its exact deadline. Logical lookups still reject it according to the
exact deadline and cached second.

### Cleanup Cursor and Batches

`cleanedMinute` is the latest minute fully processed. Cleanup walks forward
through `cleanedMinute + 1` up to `safeMinute`, in ascending order.

One call processes at most `workerBatchSize` units, where a unit is either:

- one bucket record removed, or
- one empty minute advanced.

`workerBatchSize` is 4,096. If a bucket is only partially processed, the cursor
does not advance past it. The next batch resumes the same bucket. Empty-minute
catch-up is also bounded, which prevents a large clock jump from producing one
unbounded lock hold.

For every selected bucket record, cleanup first removes the source placement.
It removes `entries[key]` and decrements its type count only when the
authoritative pointer is the same pointer. This identity check prevents an old
record from deleting a newer same-key value.

Inserting a record into a minute at or before `cleanedMinute` rewinds the
cursor to one minute before that bucket. This protects records created after a
backward wall-clock adjustment from becoming permanently stranded behind the
cursor.

## Worker Lifecycle

`New` creates one `cacheState`, registers runtime cleanup on the public wrapper,
and starts one worker:

```go
state := newCacheState(config, clock)
local := &Cache{state: state}
runtime.AddCleanup(local, stopWorker, state)
go state.runWorker()
```

The worker owns one one-second ticker. On a tick it:

1. Samples the clock and atomically publishes `cachedNowUnix`.
2. Calculates the latest safe cleanup minute.
3. Runs one cleanup batch when work is eligible.
4. Continues pending batches while checking for stop and newer tick events
   between batches.
5. Calls `runtime.Gosched` between immediate batches so other goroutines can
   run.

Each batch takes the write lock independently. The worker retains no entry or
bucket pointer across an unlock, allowing `Clear` to replace all structures
safely.

### Automatic Shutdown

The worker captures `*cacheState`, never `*Cache`. If it captured the wrapper,
the wrapper would remain reachable through the worker and its runtime cleanup
could never run.

When the wrapper becomes unreachable, `runtime.AddCleanup` may invoke
`stopWorker(state)`. `stopOnce` closes the stop channel at most once, the
worker exits, and the remaining state becomes reclaimable. Runtime cleanup is
eventual and is not guaranteed to run before process exit.

Every public method copies `c.state` locally and defers `runtime.KeepAlive(c)`.
This prevents runtime cleanup from stopping maintenance while a method is
still operating on the state.

`Clear` only removes records. It does not signal the stop channel or otherwise
alter worker lifecycle.

## Concurrency Model

- `mu` protects `entries`, `minuteBuckets`, `cleanedMinute`, and `typeCounts`.
- `Get` and `Stats` use the read lock.
- Mutations and cleanup use the write lock.
- `cachedNowUnix` is published and loaded atomically.
- The injected `clock` is read directly only when creating deadlines,
  refreshing the cached clock, or resetting the cleanup cursor.
- There is no package-global mutable state.

A single lock is intentional: the authoritative record, expiration placement,
and accounting update form one transaction. Sharding would require a clear
contention benefit and a design that preserves those cross-index invariants.

## Complexity and Space

| Path | Expected work |
| --- | --- |
| `Get` | O(1) |
| `Set` | O(1) |
| `Touch` | O(1) |
| `Delete` | O(1) |
| `Clear` | O(1) map replacement |
| `Stats` | O(t) map copy for `t` represented types |
| Cleanup | O(m + k) for elapsed minute positions and processed records |

Logical index space is O(n) entries plus O(b) occupied minute buckets. Go map
backing storage may retain a previous high-water allocation after deletions.
`Clear` replaces the maps, allowing old backing storage to be reclaimed.

## Failure Modes and Limits

- Clock refresh delay can extend logical visibility past wall-clock expiry.
- Cleanup delay can keep logically expired values and their memory resident.
- A backward wall-clock change can extend or revive entries still resident.
- Large mutable values can race independently of the cache's own locking.
- A large cleanup backlog is bounded per lock hold but may take many batches.
- Automatic worker cleanup is nondeterministic and not a process-shutdown hook.
- There is no pressure-based eviction when memory usage grows.

These are deliberate tradeoffs of a small process-local TTL cache. Applications
that require strict memory bounds, monotonic deadlines, immediate reclamation,
or coordinated invalidation need an additional policy or a different cache.

## Verification

The test suite uses an injected clock rather than long sleeps. It covers:

- TTL normalization, clamping, boundary behavior, and integer saturation
- bare nil and typed nil values
- replacement, deletion, and movement between minute buckets
- exact type counts and independent statistics snapshots
- cleanup safety boundaries, partial batches, and empty-minute catch-up
- stale bucket identity protection and backward-clock cursor recovery
- `Clear` reuse and races with mutation and cleanup
- randomized operations checked against a reference model
- concurrent operations and expiration-index invariants
- deterministic worker ticks, stop behavior, and automatic runtime cleanup

Run the standard, race, and static-analysis checks with:

```sh
go test ./...
go test -race ./...
go vet ./...
```