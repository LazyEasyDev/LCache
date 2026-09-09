# LCache

LCache is a small, in-process cache for Go. It stores values of any type,
supports per-entry TTLs, and is safe for concurrent use. It is intended for
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
is `cache`.

## Quick Start

```go
package main

import (
	"fmt"

	"github.com/LazyEasyDev/LCache"
)

func main() {
	local := cache.New(cache.DefaultConfig())

	local.Set("user:42", "Alice", 60)

	value, found := local.Get("user:42")
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
config := cache.DefaultConfig()
config.MaxTTLSeconds = 30 * 60

local := cache.New(config)
```

`MaxTTLSeconds` is the largest TTL accepted by `Set` and `Touch`.

- The default is 86,400 seconds (24 hours).
- Valid custom values are from 1 through 86,400 seconds.
- Zero, negative, and above-default values fall back to 86,400 seconds.
- A requested TTL above the configured maximum is clamped to that maximum.

`New` always returns a cache and starts one internal maintenance worker.

## API

### `Set`

```go
local.Set(key, value, ttlSeconds)
```

Stores or replaces a value. Values may have any Go type, including bare or
typed nil values. A TTL of zero or less is a no-op, so it does not insert a
new key or replace an existing value.

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
is in its final partial second.

### `Touch`

```go
found := local.Touch(key, ttlSeconds)
```

Changes the TTL of a live entry without changing its value.

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
usable, and its maintenance worker continues running.

### `Stats`

```go
stats := local.Stats()
fmt.Println(stats.Total)
fmt.Println(stats.Count(reflect.TypeOf("")))

for _, typeCount := range stats.ByType {
	fmt.Printf("%v: %d\n", typeCount.Type, typeCount.Count)
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
	Total  int
	ByType []TypeCount
}

type TypeCount struct {
	Type  reflect.Type
	Count int
}
```

`Count` returns the resident count for one `reflect.Type`. It returns zero when
the type is absent. Use `stats.Count(nil)` for bare nil values. `ByType` is
ordered from highest to lowest count. Types with equal counts have no
guaranteed relative order.

`ToJSON` preserves the `ByType` order and returns compact JSON:

```json
{"total":3,"byType":[{"type":"string","count":2},{"type":null,"count":1}]}
```

Concrete types are encoded using `reflect.Type.String()`. A bare nil type is
encoded as JSON `null`.

The counts describe physically resident records. They can temporarily include
expired entries that already produce misses but have not reached background
cleanup. A bare nil value is counted under a nil `reflect.Type`; a typed nil
uses its concrete type.

## Expiration Semantics

An entry is expired when:

```go
expiresAtUnix <= cachedNowUnix
```

Equality is expired. The cache initializes its clock during construction and
refreshes it about once per second. Reads avoid a `time.Now` call, but an
entry may remain visible briefly after its wall-clock deadline if the worker
has not refreshed the cached time. Scheduler delays or process suspension can
extend that lag.

When creating a deadline, the cache adds one second after the TTL because
`time.Now().Unix()` truncates fractional seconds. Under a normally advancing
clock, an entry therefore remains live for at least the requested number of
whole seconds.

Logical expiration and physical cleanup are separate:

- `Get`, `GetWithTTL`, `Touch`, and `Delete` use the exact second-based
	deadline.
- The worker removes old records in minute buckets and bounded batches.
- Physical removal normally occurs within about one minute after the exact
	deadline, and later if the worker is delayed or has a backlog.

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

Empty strings are valid keys. Methods must be called on a non-nil cache
returned by `New`.

## Concurrency and Lifecycle

All public operations are safe to call concurrently. One cache-level mutex
keeps entries, expiration buckets, and type counts consistent, while the
cached clock uses an atomic integer.

Share a cache by copying its pointer:

```go
alias := local
```

Do not copy the `Cache` struct itself with `copied := *local`. The type carries
a `noCopy` marker so `go vet` can report accidental copies.

There is no manual `Close` method. When the `*Cache` wrapper becomes
unreachable, a runtime cleanup signals the worker to stop. Cleanup timing is
nondeterministic and is not guaranteed before process exit.

## Complexity

| Operation | Expected cost |
| --- | --- |
| `Get` | O(1) |
| `GetWithTTL` | O(1) |
| `Set` | O(1) |
| `Touch` | O(1) |
| `Delete` | O(1) |
| `Clear` | O(1) map replacement |
| `Stats.Count` | O(t), where `t` is the number of represented value types |
| `Stats.ToJSON` | O(t) |
| `Stats` | O(t log t) to copy and order represented value types |
| Cleanup | O(m + k) across elapsed minutes and removed records |

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

## Verification

```sh
go test ./...
go test -race ./...
go vet ./...
```

## License

[MIT](LICENSE)