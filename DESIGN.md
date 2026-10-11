# LCache Design

This document describes the implemented architecture and behavioral contracts
of LCache. The package is an in-process, concurrent L1 cache for heterogeneous
Go values with second-based TTLs and automatic background cleanup.

Target Go version: 1.25.13.

## Design Summary

LCache separates expiration into two concerns:

1. **Logical expiration** decides whether an operation can observe an entry.
	It compares an exact Unix-second deadline with a time sample. `Get` samples
	the system clock; `Touch` and `Delete` use periodically refreshed cached time.
2. **Physical cleanup** reclaims expired records later. One worker scans sparse
   buckets keyed by absolute Unix minute and removes records in bounded batches.

Keys are partitioned by a seeded hash into a configurable number of shards.
Each shard's authoritative key map, expiration index, and tag counters are
updated together under its own mutex. Entry metadata is immutable after
publication, so a pointer comparison can distinguish a current entry from an
obsolete record. One worker services all shards.

An independent clock worker updates the cached time every second without
acquiring shard locks. The cleanup worker checks for eligible records every
minute and drains each shard's eligible work in bounded batches before moving
to the next shard.

Whole-cache operations acquire every shard lock in index order and release
them in reverse order. This preserves atomic `Clear`, `Close`, and `Stats`
without adding a shared lock to normal key operations.

This design gives normal cache operations expected O(1) work, avoids a timer
or goroutine per key, and keeps cleanup lock holds bounded by a work count.

## Goals

- Concurrent `Get`, `Set`, `Touch`, `Delete`, `Clear`, `Close`, and
	`Stats` operations.
- Expected O(1) lookup, mutation, and expiration-index maintenance.
- Exact second-based logical expiration and wall-clock-valid lookup snapshots.
- One expiration-index record for every physically resident entry.
- Direct removal of old expiration placements on replacement and deletion.
- Bounded cleanup work per write-lock acquisition.
- Exact resident counts grouped by caller-defined tags.
- Automatic worker shutdown after the public cache becomes unreachable.
- Atomic removal of all records with `Clear`, independent of the entry count.
- Permanent cache closure and synchronous worker shutdown with `Close`.

## Non-goals

- Memory-size or entry-count capacity limits.
- LRU, LFU, admission, or other capacity-eviction policies.
- Deep copying, ownership enforcement, or synchronization of stored values.
- Distributed invalidation or coherence with another cache.
- Exact-second physical reclamation.
- Monotonic TTL behavior across wall-clock corrections.
- Automatic shrinking of Go map backing storage after high-water usage.

## Public Contract

The exported surface is:

```go
const DefaultMaxTTLSeconds int64 = 24 * 60 * 60
const DefaultShardCount = 32
const MaxShardCount = 1_024

type Config struct {
	MaxTTLSeconds int64
	ShardCount    int
}

func DefaultConfig() Config

type TagCount struct {
	Tag   string `json:"tag"`
	Count int    `json:"count"`
}

type Stats struct {
	Total int        `json:"total"`
	ByTag []TagCount `json:"byTag"`
}

func (s Stats) Count(tag string) int
func (s Stats) ToJSON() ([]byte, error)

type Result struct {
	value            any
	expiresAtUnix    int64
	remainingSeconds int64
	found            bool
}

func (result Result) Val() any
func (result Result) Found() bool
func (result Result) ExpiresAtUnix() int64
func (result Result) RemainingSeconds() int64

func Init(config Config) *Cache
func Set(key string, value any, ttlSeconds int64, tag string)
func Get(key string) Result
func Touch(key string, ttlSeconds int64) bool
func Delete(key string) bool
func Clear()
func GlobalStats() Stats
func Close()

func New(config Config) *Cache
func (c *Cache) Set(key string, value any, ttlSeconds int64, tag string)
func (c *Cache) Get(key string) Result
func (c *Cache) Touch(key string, ttlSeconds int64) bool
func (c *Cache) Delete(key string) bool
func (c *Cache) Clear()
func (c *Cache) Close()
func (c *Cache) Stats() Stats
```

`New` is infallible. It normalizes `MaxTTLSeconds` to the 24-hour default when
the configured value is outside the inclusive range `1..86400`. `ShardCount`
accepts any value in `1..1024`; other values use `DefaultShardCount` (32).
The count is fixed for the lifetime of the cache. A count of one bypasses the
routing hash.

`Init` creates and assigns the package-level cache only when the global pointer
is nil. Repeated calls return the existing `*Cache`, ignore the supplied config,
and preserve its entries and workers. Package-level `Close` synchronously closes
the instance and clears the global pointer, allowing a later `Init` to create
a fresh cache with a new config. Calling the returned instance's `Close` method
directly does not clear the global pointer; `Init` continues to return that
closed instance until package-level `Close` is called.

The global pointer is not synchronized; package-level data operations delegate
directly to `*Cache` and rely on its internal locking. Callers must initialize
before starting global cache users and wait for all users to finish before
closing it.
`Init` and package-level `Close` must not run concurrently with any package-level
cache operation, including each other. Before `Init` and after package-level
`Close`, writes are no-ops, reads are misses, and `GlobalStats` is empty. `New`
remains available for independent cache instances.

### Lookup Results

`Get` returns a `Result` by value with private fields. It selects the key's
shard and takes its read lock. A missing record produces a zero-value result.
For a resident record, it samples `max(time.Now().Unix(), 0)` once and rejects
a deadline at or before that sample. For a live record, it copies the value
and deadline and computes positive remaining seconds before releasing the lock.

`Val()` returns the stored value, `Found()` reports a live hit,
`ExpiresAtUnix()` returns the absolute deadline, and `RemainingSeconds()`
returns the deadline minus that lookup's Unix-second sample. Missing and
expired records expose neither a value nor TTL metadata: all result fields
are zero, with `Found()` false. A stored nil still has `Found()` true.

The methods read only the snapshot; they do not sample time or access cache
state. Expiration, replacement, `Touch`, `Delete`, `Clear`, and `Close` cannot
change an existing result. This is not a guarantee of validity when the result
is inspected later. Reference-bearing values remain caller-owned and are not
cloned. There is no separate TTL lookup API.

### Operation Semantics

| Operation | Live key | Expired key | Missing key |
| --- | --- | --- | --- |
| `Get` | returns `Result` with value, deadline, positive remaining seconds, and `Found()` true | returns zero-value `Result` | returns zero-value `Result` |
| `Set`, positive TTL | replaces value, deadline, and tag | replaces value, deadline, and tag | inserts value, deadline, and tag |
| `Set`, non-positive TTL | no-op | no-op | no-op |
| `Touch`, positive TTL | replaces deadline, returns `true` | no-op, returns `false` | returns `false` |
| `Touch`, non-positive TTL | no-op, returns `true` | no-op, returns `false` | returns `false` |
| `Delete` | removes record, returns `true` | removes record, returns `false` | returns `false` |

For `Get`, live and expired status uses the system-clock sample taken during
the lookup. `Touch` and `Delete` retain their cached-clock checks, so they may
still consider a record live after `Get` rejects it until the clock refreshes.

`Clear` atomically replaces every shard's record and index maps with empty
maps. Calls ordered after it by their shard lock observe an empty cache. The
cache remains usable and the worker lifecycle is unchanged. After `Close`,
`Clear` is a no-op.

`Close` permanently empties the cache and waits for both workers and tickers to
stop. It is safe to call repeatedly and concurrently with all other operations.
Operations ordered after closure by their shard lock observe an empty cache:
`Set` and `Clear` are no-ops, `Get` returns a zero-value `Result`,
`Touch` and `Delete` return `false`, and `Stats` is empty. A closed
cache cannot be reopened. Stored values remain caller-owned and are not closed.

`Stats` returns a snapshot with a newly allocated `ByTag` slice sorted from
highest to lowest count; equal-count ordering is unspecified. Mutating that
slice cannot affect the cache. `Stats.Count` scans the snapshot and returns
zero when the tag is absent or the snapshot is the zero value. Counts include
all physically resident records, including logically expired records awaiting
cleanup.

`Stats.ToJSON` encodes `Total` as `total` and the ordered `ByTag` slice as
`byTag`, with `tag` and `count` fields on each item. An empty snapshot encodes
`byTag` as `[]`, not `null`. The returned bytes are independently allocated.

Tags are explicit, case-sensitive labels supplied to `Set`. The empty string
is valid and counted. Values of different Go types may share a tag, and values
of the same type may use different tags. Tags are not part of key identity or
shard routing. A same-key overwrite replaces its tag; `Touch` preserves it.

### Keys and Values

- The empty string is a valid key.
- Values are accepted as `any`, including bare and typed nil values.
- The value returned by `Result.Val()` needs a caller-side type assertion.
- `Result.Found()` distinguishes a stored nil from a miss.
- Values are stored directly and are not cloned.
- Callers own synchronization for mutable pointers, maps, slices, and other
  reference-bearing values.
- A stored value must not reference its owning `*Cache`, directly or
	indirectly. LCache does not inspect object graphs to enforce this rule.
- Calling a method on a nil `*Cache` is a programming error and may panic.

### Copying

`Cache` must not be copied after construction. Pointer aliases are supported:

```go
alias := local
```

Copying the wrapper with `copied := *local` is unsupported because runtime
cleanup is registered on the original wrapper. A conventional `noCopy` field
allows `go vet` to detect common accidental copies.

`Result` may be copied freely. It retains no cache pointer and does not refer
to mutable entry metadata, although its value may contain caller-owned references.

## Data Model

The implementation centers on these structures:

```go
type entry struct {
	value         any
	expiresAtUnix int64
	tag           string
}

type cacheShard struct {
	mu            sync.RWMutex
	entries       map[string]*entry
	minuteBuckets map[int64]map[string]*entry
	cleanedMinute int64
	closed        bool
	tagCounts     map[string]int
}

type cacheState struct {
	shards        []cacheShard
	hashSeed      maphash.Seed
	cachedNowUnix atomic.Int64
	maxTTLSeconds int64

	stop       chan struct{}
	stopOnce   sync.Once
	clockDone  chan struct{}
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

Within each shard, `entries` is the authoritative key-to-record map.
`minuteBuckets` is a derived expiration index, and `tagCounts` is derived
accounting. All three are protected by that shard's `mu`.

`maphash.String` hashes each key using a per-cache random seed. Its result
modulo the shard count selects the owner. The seed and shard array are immutable
after construction, so routing requires no synchronization. The empty string
and arbitrary string bytes are valid keys. A key never moves between shards
within the same cache.

Every positive `Set` and successful positive `Touch` creates a new `entry`.
The value, deadline, and tag metadata are not changed after the entry is
published. Accounting uses the stored tag, not reflection on the value.
Reference-bearing values may still point to mutable data; metadata immutability
does not imply deep value immutability.

### Invariants

At every public operation boundary while the owning shard's `mu` is held:

1. Every record in `entries` appears under the same key in exactly one minute
   bucket.
2. The selected bucket key equals `entry.expiresAtUnix / 60`.
3. Every bucket record points to the authoritative record for that key.
4. `minuteBuckets` contains no empty bucket.
5. `tagCounts[tag]` equals the number of resident entries whose stored tag
   matches; zero counts are absent.
6. Each key appears only in the shard selected by its routing hash.

Bare nil and typed nil values are counted under their supplied tag just like
other values. The sum of tag counts equals the number of resident entries.

## Time and Expiration

### Exact Deadline

For `Get`, an entry is live precisely when:

```go
nowUnix := max(time.Now().Unix(), 0)
entry.expiresAtUnix > nowUnix
```

Equality is expired. The result retains the stored deadline as an absolute
Unix-second value and calculates remaining seconds from that same system-clock
sample:

```go
remainingSeconds = expiresAtUnix - nowUnix
```

Every hit has positive remaining seconds. The final partial second is counted
as one second. Misses return a zero-value result rather than expired metadata.
`Touch` and `Delete` use the same strict deadline comparison against cached time.

`Set` and positive `Touch` calculate a deadline from the cached clock with one
extra second to allow for Unix-second truncation:

```go
expiresAtUnix = cachedNowUnix + ttlSeconds + 1
```

The calculation saturates at `math.MaxInt64`. TTL input is already capped at
24 hours, but saturation protects deadline arithmetic if the system clock
approaches the integer limit. Because the clock is sampled rather than read
at the moment of each write, this is not a strict minimum wall-clock lifetime.
The padding is unchanged, so a fresh lookup can report one more remaining
second than the effective TTL supplied to `Set` or `Touch`.

### Cached Clock

`New` initializes `cachedNowUnix` synchronously. The independent clock worker
refreshes it from `time.Now().Unix()` about once per second using `atomic.Int64`.
It acquires no shard locks, so cleanup cannot block clock publication.

`Touch` and `Delete` use this cached value to decide
whether an entry is live. They therefore avoid a system clock call, but can
observe an entry as live briefly after its wall-clock deadline. The normal lag
is around one second; scheduling delays and process suspension mean there is
no hard upper bound. `Get` instead samples the system clock under the shard's
read lock, so its validity check does not depend on clock-worker scheduling.

`Set` and positive `Touch` also use an atomic clock load when creating a
deadline; `Clear` uses it when resetting cleanup cursors. `Touch` uses the same
sample to check the old entry and calculate its new deadline. Construction,
resident-key `Get` lookups, and worker refreshes call `time.Now().Unix()`.

A stale sample can shorten a newly assigned TTL relative to wall-clock time.
In particular, if clock scheduling is delayed, `Get` can immediately reject
an entry whose new deadline is already expired against the system clock.
The extra second does not compensate for arbitrary scheduling delays.

Expiration follows the system wall clock. A backward adjustment can extend or
revive a physically resident entry. The design does not provide monotonic TTL
semantics. Clock readings before the Unix epoch are normalized to Unix second
zero because negative wall-clock time is outside the cache's supported runtime
domain.

## Mutation Transactions

### Set

For a positive TTL, `Set`:

1. Clamps the TTL to `maxTTLSeconds`.
2. Selects the key's shard and acquires its `mu` for writing.
3. Loads the cached clock and creates a new immutable record with the supplied tag.
4. Removes any previous record's bucket placement only when its expiration
	minute changes.
5. Increments the tag count for a new key. If an existing key changes tags,
	decrements the old tag and increments the new one; same-tag replacements
	leave counts unchanged regardless of the value's Go type.
6. Publishes the new record in `entries` and its due-minute bucket.

A non-positive TTL returns before locking and changes nothing, including when
the key already exists.

### Touch

`Touch` acquires the owning shard's write lock and loads the cached clock once.
It checks the existing record against that sample and returns `false` without
mutation for an absent or expired record. A live record with a non-positive
requested TTL returns `true` without mutation.

For a live record and positive TTL, it uses the same clock sample to create a
new record carrying the same value and tag. It removes the old bucket placement
only when the expiration minute changes, replaces the authoritative pointer,
and writes the new bucket placement, reusing the existing bucket for same-minute
updates. Tag counts do not change.

### Delete

`Delete` acquires the owning shard's write lock and records whether the resident entry is
live. If present, it removes the authoritative entry, its matching bucket
placement, and its tag count. It returns the previously determined live
status. This is why deleting an expired but resident record returns `false`
while still reclaiming it.

### Clear

`Clear` acquires every shard's write lock in index order, then swaps each
shard's `entries`, `minuteBuckets`, and `tagCounts` for new empty maps. It also
resets each shard's cursor using one cached-clock load:

```go
cleanedMinute = unixMinute(cachedNowUnix) - cleanupGraceMinutes
```

The operation does not iterate over old records, call `runtime.GC`, stop the
worker, or retain old bucket references after unlocking. Old structures become
eligible for garbage collection when no concurrent operation references them.

## Minute-Bucket Index

Each entry is placed in the bucket for the absolute Unix minute containing its
deadline:

```go
dueMinute := expiresAtUnix / 60
```

Buckets are sparse. The outer map and an inner key map allocate storage only
for occupied minutes. Same-minute `Set` and `Touch` updates overwrite the
existing bucket placement without deleting or recreating the bucket, even
when it contains only one entry. An empty bucket is removed immediately after
a cross-minute replacement or deletion. Absolute minute keys avoid
circular-slot reuse, overflow buckets, and range remapping.

The ordinary future horizon is bounded by the configured maximum TTL. The
index can retain expired records during the cleanup grace period or a worker
backlog.

### Cleanup Safety Boundary

At the start of each `cleanExpired` call, the cleanup worker computes one cutoff
and retains it for the entire pass:

```go
safeMinute := unixMinute(state.cachedNowUnix.Load()) - cleanupGraceMinutes
```

`cleanupGraceMinutes` is 1. A bucket is eligible only when its minute is at or
before `safeMinute`. With no backward clock adjustment during the pass, every
second in an eligible bucket is in the past, so cleanup does not perform
another deadline comparison.

The fixed cutoff has a known wall-clock limitation: if the clock moves backward
while a pass is running, a newly written entry can be live against the newer
cached sample yet belong to a bucket eligible under the old cutoff. That pass
can remove the live entry. Rewinding the cursor on insertion prevents stranded
buckets, but does not prevent this premature removal.

With minute-based eligibility and a once-per-minute cleanup ticker, an entry
normally remains physically resident for up to about two minutes after its
exact deadline. Cleanup backlog and scheduling delays can extend this time.
`Get` still rejects it according to the deadline and sampled system time.

### Cleanup Cursor and Batches

`cleanedMinute` is the latest minute fully processed. Cleanup walks forward
through `cleanedMinute + 1` up to `safeMinute`, in ascending order.

One shard cleanup batch processes at most `workerBatchSize` units, where a unit
is either:

- one bucket record removed, or
- one empty minute advanced.

`workerBatchSize` is 4,096. If a bucket is only partially processed, the cursor
does not advance past it. The next batch resumes the same bucket. Empty-minute
catch-up is also bounded, which prevents a large clock jump from producing one
unbounded lock hold.

`cleanExpired` visits shards in index order. For each shard it repeatedly takes
the write lock, compares the cursor with the fixed cutoff, and calls
`cleanExpiredBatchLocked` until the cursor reaches that cutoff. Both functions
return no continuation flag; the cursor decides whether another batch is needed.

The write lock is released after each batch. Shutdown is checked before each
lock acquisition, and `runtime.Gosched` voluntarily yields after unlocking so
other runnable goroutines have a scheduling opportunity. It does not sleep,
wait for a ticker, or guarantee fairness. Work is bounded per lock acquisition,
not per entire pass; a large backlog on one shard can delay cleanup of later
shards. Clock refreshes proceed independently of this work.

For every selected bucket record, cleanup first removes the source placement.
It removes `entries[key]` and decrements its stored tag's count only when the
authoritative pointer is the same pointer. This identity check prevents an old
record from deleting a newer same-key value.

Inserting a record into a minute at or before `cleanedMinute` rewinds the
cursor to one minute before that bucket. This protects records created after a
backward wall-clock adjustment from becoming permanently stranded behind the
cursor.

## Worker Lifecycle

`New` initializes one `cacheState` with the current wall-clock time and the
configured shards. It then registers runtime cleanup on the public wrapper
and starts two workers:

```go
cache := &Cache{state: state}
runtime.AddCleanup(cache, stopWorker, state)
go state.runClockUpdate()
go state.runWorker()
```

The clock worker owns a one-second ticker. On each tick it samples the system
clock and atomically publishes `cachedNowUnix`, without acquiring shard locks.
It stops its ticker before closing `clockDone` on exit.

The cleanup worker owns a one-minute ticker. On a tick it:

1. Loads the cached clock once and calculates the pass's fixed cleanup cutoff.
2. Visits each shard in index order and drains its eligible work in batches.
3. Checks for shutdown before each batch and stops the pass when requested.
4. Releases the shard lock and calls `runtime.Gosched` after each batch before
	continuing immediately, without waiting for another ticker event.

Each shard batch takes its write lock independently. The cleanup worker retains
no entry or bucket reference across an unlock, allowing `Clear` to replace all
structures safely and `Close` to release them. It stops its ticker before closing
`workerDone` to signal cleanup shutdown completion.

### Explicit Shutdown

`Close` performs shutdown in three steps:

1. Calls `stopWorker` to signal both background workers.
2. Waits for `clockDone` and `workerDone` without holding shard locks, allowing
	any in-progress cleanup batch to finish against intact maps before the worker
	observes shutdown.
3. Acquires every shard's write lock in index order, marks each shard closed,
	releases its entry, bucket, and tag-count maps, and unlocks in reverse order.

Cleanup has already stopped when the maps are cleared, so its batches do not
need a closed-state check. `Set` and `Clear` retain their checks: concurrent
writes either complete before closure and are discarded or become no-ops,
and `Clear` cannot reopen the cache. Reads, `Touch`, `Delete`, and `Stats`
observe empty maps through their existing locked paths after closure.

Every concurrent or repeated `Close` waits for both workers. `stopOnce` ensures
the stop channel is closed only once, including when automatic cleanup also
requests shutdown.

The cached clock stops advancing when the clock worker exits. Cached-clock
operations overlapping shutdown may still use the final sample until the maps
are cleared. `Get` continues to sample system time for resident records.
Before `Close` returns, the cache is empty and rejects further writes.

### Automatic Shutdown

Both workers capture `*cacheState`, never `*Cache`. If either captured the
wrapper, the wrapper would remain reachable through that worker and its runtime
cleanup could never run.

When the wrapper becomes unreachable, `runtime.AddCleanup` may invoke
`stopWorker(state)`. `stopOnce` closes the stop channel at most once, the
workers exit, and the remaining state becomes reclaimable. Runtime cleanup is
eventual and is not guaranteed to run before process exit.

A cached value that points back to its owning `*Cache`, including through a
nested object or closure, creates a path from the worker state back to the
wrapper. That path keeps the wrapper reachable and delays its runtime cleanup
until expiration cleanup, `Delete`, `Clear`, or `Close` removes the value. Callers must
therefore not store the owning cache in its own value graph.

Every public `*Cache` method copies `c.state` locally and defers `runtime.KeepAlive(c)`.
This prevents runtime cleanup from stopping maintenance while a method is
still operating on the state.

`Clear` only removes records. It does not signal the stop channel or otherwise
alter worker lifecycle.

## Concurrency Model

- Each shard's `mu` protects its `closed`, `entries`, `minuteBuckets`,
	`cleanedMinute`, and `tagCounts`.
- `Get` uses only the owning shard's read lock and samples the system clock there.
- Key mutations and cleanup use only the owning shard's write lock.
- `Stats` acquires all read locks in index order, copies and aggregates counts,
	releases the locks in reverse order, then sorts the independent snapshot.
- `Clear` and `Close` acquire all write locks in index order before changing
	any shard, and release them in reverse order.
- `cachedNowUnix` is published and loaded atomically.
- `time.Now().Unix()` is called during construction, resident-key `Get` lookups,
	and background clock refreshes. Other operations load the cached sample.
- `Result` methods only read copied snapshot fields and do not access cache state
	or sample time again.
- The optional package-level `*Cache` has no extra synchronization. Callers
	synchronize `Init` and package-level `Close` with each other and all global
	cache users; data operations use only the instance's existing locks.

The authoritative record, expiration placement, and accounting update still
form one transaction within a shard. Keys routed to different shards can
proceed independently. Additional hashing can cost more for serial traffic;
one hot key still contends on one shard. More shards require more metadata and
more lock acquisitions for whole-cache operations.

## Complexity and Space

Here `s` is the shard count, `g` is the number of resident tags, and `q` is
the sum of distinct resident tag counts across individual shards. Key and tag
hashing also costs work proportional to their byte lengths.

| Path | Expected work |
| --- | --- |
| `Get` | O(1) |
| `Result` methods | O(1) |
| `Set` | O(1) |
| `Touch` | O(1) |
| `Delete` | O(1) |
| `Clear` | O(s) map replacement, independent of entry count |
| `Close` | O(s) map release, plus waiting for worker shutdown |
| `Stats.Count` | O(g) for `g` resident tags |
| `Stats.ToJSON` | O(g), plus tag-string serialization |
| `Stats` | O(s + q + g log g) to aggregate and order tag counts |
| Cleanup | O(s + m + k) for a pass's shards, minute positions, and records |

Logical index space is O(n + b + s) for entries, occupied minute buckets, and
shard metadata. Go map
backing storage may retain a previous high-water allocation after deletions.
`Clear` replaces the maps, allowing old backing storage to be reclaimed.

## Failure Modes and Limits

- Clock refresh delay can assign stale write deadlines and delay `Touch` or
	`Delete` expiration decisions, but does not delay `Get` expiration checks.
- Cleanup delay can keep logically expired values and their memory resident.
- A backward wall-clock change can extend or revive entries still resident.
- Large mutable values can race independently of the cache's own locking.
- A large cleanup backlog is bounded per lock hold but may take many batches.
- Automatic worker cleanup is nondeterministic and not a process-shutdown hook.
- There is no pressure-based eviction when memory usage grows.

These are deliberate tradeoffs of a small process-local TTL cache. Applications
that require strict memory bounds, monotonic deadlines, immediate reclamation,
or coordinated invalidation need an additional policy or a different cache.