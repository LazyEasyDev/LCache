# LCache

LCache is a small, in-process cache for Go. It stores values of any type,
supports per-entry TTLs and concurrent data access. It is intended for
fast L1 caching inside an application, often in front of a remote cache such
as Redis.

The cache uses exact Unix-second deadlines for lookups and sparse
minute-based buckets for background cleanup. This keeps normal reads and
writes at expected O(1) cost without creating a timer or goroutine for every
entry.

## Requirements

- Go 1.25.13 or later
- No third-party dependencies

## Installation

```sh
go get github.com/LazyEasyDev/LCache
```

The module path is `github.com/LazyEasyDev/LCache`; the imported package name
is `LCache`.

## Quick Start

```go
package main

import (
	"fmt"

	"github.com/LazyEasyDev/LCache"
)

func main() {
	LCache.Init(LCache.DefaultConfig())
	defer LCache.Close()

	LCache.Set("user:42", "Alice", 60, "users")

	value, found := LCache.Get("user:42")
	if !found {
		fmt.Println("cache miss")
		return
	}

	name, ok := value.(string)
	if !ok {
		fmt.Println("unexpected value type")
		return
	}

	fmt.Println(name)
}
```

## Configuration

```go
config := LCache.DefaultConfig()
config.MaxTTLSeconds = 30 * 60
config.ShardCount = 64

LCache.Init(config)
```

`MaxTTLSeconds` is the largest TTL accepted by `Set` and `Touch`.

- The default is 86,400 seconds (24 hours).
- Valid custom values are from 1 through 86,400 seconds.
- Zero, negative, and above-default values fall back to 86,400 seconds.
- A requested TTL above the configured maximum is clamped to that maximum.

`ShardCount` controls the number of independently locked key maps.

- The default is `DefaultShardCount`, currently 32.
- Any count from 1 through `MaxShardCount` (1,024) is supported; powers of two
	are not required.
- Zero, negative, and above-maximum values use `DefaultShardCount`.
- Use 1 for a single-shard cache without the extra routing hash.
- The count is fixed when the cache is created; changing the config later does
	not resize an existing cache.

Each shard owns its entries, TTL buckets, and tag counts. A seeded key hash
selects one shard for each data operation, allowing unrelated keys to use
different locks. One cleanup worker handles all shards, and a separate clock
worker refreshes the cached time without acquiring shard locks. `Clear`,
`Close`, and `Stats` lock all shards in a fixed order to preserve atomic
whole-cache behavior.

Sharding primarily helps concurrent traffic spread across different keys.
Single-threaded access can be slower because routing adds hashing work, and
one hot key still uses one lock. More shards also increase metadata and the
cost of whole-cache operations. Start with the default and adjust `ShardCount`
to suit the application's concurrency and key distribution.

`Init` creates the package-level cache and starts its clock and cleanup workers
only when no global instance exists. Repeated calls return the same
`*Cache`, preserving its entries, configuration, and workers; new configuration
arguments are ignored. To apply a different configuration, stop all global
cache users, call package-level `Close`, then call `Init` with the new config.

Use `New` when an application needs independent cache instances:

```go
local := LCache.New(config)
defer local.Close()
```

## Global Cache

After `Init`, the package-level `Set`, `Get`, `GetWithTTL`, `Touch`, `Delete`,
`Clear`, and `GlobalStats` functions operate on the initialized cache. `Close`
stops and removes it; a later `Init` creates a fresh cache. Closing the `*Cache`
returned by `Init` directly does not clear the global reference, so subsequent
`Init` calls still return that closed instance. Use package-level `LCache.Close()`
to reset the global instance.

Calls are safe before initialization: writes are no-ops, reads are misses, and
`GlobalStats` returns an empty snapshot. Once initialized, package-level data
operations are safe to call concurrently through the cache's internal locking;
the global facade adds no synchronization.

Callers own the global lifecycle: call `Init` before starting goroutines that
use the global cache, and wait for all users to finish before shutting it down
with package-level `Close`. Call `Init` again after `Close` to create a new
instance. These lifecycle calls must not run concurrently with any package-level
cache operation, including each other. Overlapping lifecycle and data operations
can cause a data race.

## API

The examples below use `local`, a `*Cache` created with `New`. Package-level
data operations have the same semantics and use the global instance initialized
by `Init`. Use `LCache.GlobalStats()` instead of `local.Stats()` for global
statistics. Global initialization and shutdown have the caller-synchronization
requirements described in [Global Cache](#global-cache).

### `Set`

```go
local.Set(key, value, ttlSeconds, tag)
```

Stores or replaces a value. Values may have any Go type, including bare or
typed nil values. A TTL of zero or less is a no-op, so it does not insert a
new key or replace an existing value or tag.

The required fourth argument is a caller-defined statistics label. Tags are
case-sensitive strings and are not inferred from keys or value types. Use `""`
when no label is needed; empty tags are counted like any other tag.

```go
local.Set("user:1", "Alice", 60, "users")
local.Set("user:2", "Bob", 60, "users")
```

Starting from an empty cache, these calls create two entries under the `users`
tag. Replacing `user:1` under the same tag keeps that count unchanged; replacing
it with another tag moves its count to the new tag. A tag is not a namespace:
the same key always replaces the same entry, regardless of its tag.

This is a breaking API change from three-argument `Set` calls. Add a label or
`""` to each existing call. Statistics now group entries by tag rather than
Go value type; `Get`, `GetWithTTL`, `Touch`, and `Delete` keep their signatures.

### `Get`

```go
value, found := local.Get(key)
```

Returns a value only while it is live. On a miss, it returns `nil, false`.
The `found` result distinguishes a stored nil value from a missing key.

Convert the returned `any` value with a type assertion:

```go
value, found := local.Get("user:42")
if found {
	name, ok := value.(string)
	if !ok {
		// The key contains a different type.
	}
	_ = name
}
```

### `GetWithTTL`

```go
value, expiresAtUnix, remainingTTLSeconds, found := local.GetWithTTL(key)
```

Returns the same live value as `Get` together with its absolute Unix-second
deadline and remaining complete TTL seconds. On a miss, it returns
`nil, 0, 0, false`.

The remaining TTL is calculated from the same cached clock used to decide
whether the entry is live. It may be zero while `found` is true when the entry
is in its final second according to that clock. Clock refresh delays can make
the reported TTL overestimate the wall-clock time remaining.

### `Touch`

```go
found := local.Touch(key, ttlSeconds)
```

Changes the TTL of a live entry without changing its value or tag.

- Returns `false` when the key is absent or expired.
- A positive TTL replaces the deadline and is clamped when necessary.
- A zero or negative TTL leaves a live entry unchanged and returns `true`.

### `Delete`

```go
deleted := local.Delete(key)
```

Physically removes the key. It returns `true` only when the key was live. If
an expired record is still waiting for background cleanup, `Delete` removes
it but returns `false` because it was already logically absent.

### `Clear`

```go
local.Clear()
```

Atomically removes all entries and resets statistics. The cache remains
usable, and both background workers continue running. After `Close`, `Clear`
is a no-op and does not reopen the cache.

### `Close`

```go
local.Close()
```

`local.Close()` permanently empties the cache, releases its references to stored
values, and waits for both background workers and their tickers to stop. It is safe
to call repeatedly and concurrently with other methods on that instance. This
concurrency guarantee does not apply to package-level `LCache.Close()`; see
[Global Cache](#global-cache).

After `local.Close()`:

- `Set` and `Clear` are no-ops.
- `Get` returns `nil, false`.
- `GetWithTTL` returns `nil, 0, 0, false`.
- `Touch` and `Delete` return `false`.
- `Stats` returns zero total entries and no tag counts.

A closed cache cannot be reopened; use `New` to create another one. Stored
values remain caller-owned; `Close` does not call their own `Close` methods.

### `Stats`

This example uses the standard-library `fmt` package.

```go
stats := local.Stats()
fmt.Println(stats.Total)
fmt.Println(stats.Count("users"))

for _, tagCount := range stats.ByTag {
	fmt.Printf("%s: %d\n", tagCount.Tag, tagCount.Count)
}

encoded, err := stats.ToJSON()
if err != nil {
	return
}
fmt.Println(string(encoded))
```

`Stats` returns an independent snapshot:

```go
type Stats struct {
	Total int        `json:"total"`
	ByTag []TagCount `json:"byTag"`
}

type TagCount struct {
	Tag   string `json:"tag"`
	Count int    `json:"count"`
}
```

`Count` returns the resident count for one tag. It returns zero when the tag
is absent. Use `stats.Count("")` for entries with an empty tag. `ByTag` is
ordered from highest to lowest count. Tags with equal counts have no
guaranteed relative order.

For two `users` entries and one `sessions` entry, `ToJSON` preserves the
`ByTag` order and returns compact JSON:

```json
{"total":3,"byTag":[{"tag":"users","count":2},{"tag":"sessions","count":1}]}
```

An empty snapshot encodes as `{"total":0,"byTag":[]}`. Tag strings are encoded
with normal JSON escaping. Counts do not depend on the stored values' Go types.

The counts describe physically resident records. They can temporarily include
expired entries that already produce misses but have not reached background
cleanup. Bare nil and typed nil values are grouped by their supplied tag like
any other value. Deletion, cleanup, `Clear`, and `Close` remove the associated
tag counts. Statistics describe current contents, not write history or memory
usage.

## Expiration Semantics

An entry is expired when:

```go
expiresAtUnix <= cachedNowUnix
```

Equality is expired. The cache initializes its clock during construction and
refreshes it about once per second. `Set`, `Touch`, reads, `Delete`, and `Clear`
load this cached clock atomically rather than calling `time.Now`. Only
construction and the clock worker sample the system clock. Cleanup runs in a
separate goroutine and cannot block clock refreshes on shard locks.

`Set` and positive `Touch` create deadlines from the cached sample plus the
effective TTL (after clamping) and one extra second for Unix-second truncation.
TTL timing is approximate relative to wall-clock time, not a strict minimum
lifetime from the moment of the call.

An entry may remain visible after its wall-clock deadline until the cached
clock is refreshed. If the clock worker is delayed, writes also use an older sample;
the next refresh can expire a newly written entry sooner than its requested
wall-clock lifetime. Scheduler delays or process suspension can extend the
clock lag.

Logical expiration and physical cleanup are separate:

- `Get`, `GetWithTTL`, `Touch`, and `Delete` use the exact second-based
  deadline.
- The cleanup worker checks minute buckets once per minute and removes eligible
	records using one fixed cutoff per pass. It drains each shard before moving
	to the next, releasing the write lock and yielding between batches of at most
	4,096 record removals or empty-minute advances. Shutdown is checked between
	batches; pending work does not wait for another ticker event.
- Physical removal normally occurs within about two minutes after the exact
	deadline, and later if cleanup is delayed or has a backlog.

Expiration follows the system wall clock, not monotonic time. A backward clock
adjustment can extend or revive a resident entry. Cleanup currently retains
its initial cutoff for the entire pass; if the clock moves backward during
that pass, an entry written against the newer sample can be removed early.

## Values and Ownership

LCache stores the supplied value directly; it does not clone it. Strings and
other value types are copied according to normal Go assignment rules.
Pointers, maps, slices, and other reference-bearing values still refer to
caller-visible data. The caller is responsible for synchronizing concurrent
access to mutable cached values.

Do not store the owning `*Cache` as a value, either directly or through a
struct, map, closure, or other reference chain. Such a back-reference keeps
the cache wrapper reachable from its worker state and delays automatic worker
shutdown until the entry is physically removed. LCache does not attempt to
detect indirect references.

Empty strings are valid keys. Methods require a non-nil, initialized `*Cache`
returned by `New` or `Init`; the zero-value `Cache` is not usable.

## Concurrency and Lifecycle

All `*Cache` methods are safe to call concurrently. Package-level data
operations are also safe for concurrent use while the global instance is
unchanged, but package-level `Init` and `Close` require caller synchronization.
Each shard's mutex keeps its entries, expiration buckets, and tag counts
consistent, while the cached clock uses an atomic integer. Whole-cache
operations acquire all shard locks in a fixed order.

Share a cache by copying its pointer:

```go
alias := local
```

Do not copy the `Cache` struct itself with `copied := *local`. The type carries
a `noCopy` marker so `go vet` can report accidental copies.

Call `local.Close()` when an independent cache is no longer needed for
deterministic worker shutdown. When its `*Cache` wrapper becomes unreachable
without an explicit `Close`, a runtime cleanup may signal both workers to stop
as a fallback. Cleanup timing is nondeterministic and is not guaranteed before
process exit.

The global instance remains reachable through the package-level reference, so
it cannot rely on this cleanup. Stop all global cache users before calling
`LCache.Close()` to shut it down and clear that reference.

## Complexity

Here `s` is the shard count, `g` is the number of resident tags, and `q` is
the sum of distinct resident tag counts across individual shards. Key and tag
hashing also costs work proportional to their byte lengths.

| Operation | Expected cost |
| --- | --- |
| `Get` | O(1) |
| `GetWithTTL` | O(1) |
| `Set` | O(1) |
| `Touch` | O(1) |
| `Delete` | O(1) |
| `Clear` | O(s) map replacement, independent of entry count |
| `Close` | O(s) map release, plus waiting for worker shutdown |
| `Stats.Count` | O(g) |
| `Stats.ToJSON` | O(g), plus tag-string serialization |
| `Stats` | O(s + q + g log g) to aggregate and order tag counts |
| Cleanup | O(s + m + k) across shards, elapsed minutes, and removed records |

Resident index space is O(n) plus one map entry per occupied expiration
minute. Go maps can retain prior backing storage after heavy churn; `Clear`
replaces the maps so that old storage can be reclaimed.

## Scope

LCache deliberately does not provide:

- memory-size or entry-count limits
- LRU or LFU eviction
- value cloning or mutation control
- distributed invalidation
- monotonic expiration across backward wall-clock changes
- exact-second physical removal

See [DESIGN.md](DESIGN.md) for the data model, invariants, cleanup algorithm,
and lifecycle design.

## License

[MIT](LICENSE)