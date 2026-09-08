# github.com/LazyEasyDev/LCache

Status: implemented for Go 1.25.13. [DESIGN.md](DESIGN.md) is the authoritative architecture and behavior specification.

The replacement is a process-local, thread-safe L1 cache for heterogeneous Go values, typically used in front of Redis. It evaluates Unix-second deadlines against a cached clock refreshed about once per second and uses sparse minute-bucketed background cleanup without one timer or goroutine per key.

## API

```go
config := cache.DefaultConfig()
local := cache.New(config)

local.Set("name", "Alice", 60)

value, expiresAtUnix, found := local.Get("name")
if found {
	name := value.(string)
	_, _ = name, expiresAtUnix
}

local.Touch("name", 120)
local.Delete("name")
local.Clear()
```

`Set` accepts `any`, including bare and typed nil values. `Get` returns the stored value, its absolute Unix-second expiration, and a presence boolean. The default and maximum supported TTL is 24 hours. Non-positive `Set` TTLs are no-ops, and longer TTLs are clamped.

## Expiration Model

The authoritative entry map stores an exact Unix-second deadline. An entry is expired when `expiresAtUnix <= cachedNowUnix`; equality is expired. Reads use the cached clock refreshed about once per second, so minute buckets do not reduce logical expiration precision.

Positive `Set` and `Touch` operations create immutable entry metadata and update both the authoritative map and the bucket keyed by the deadline's absolute Unix minute under one write lock. Replacing or deleting a key removes its previous bucket record in the same transaction. There is no scheduling queue, intermediate unscheduled state, or numeric version.

One background worker refreshes the cached clock and removes expired records from old minute buckets in bounded batches. Pointer identity prevents cleanup from deleting a newer same-key value.

Only occupied minutes allocate bucket maps. The 24-hour maximum TTL bounds the ordinary future scheduling horizon, while absolute minute keys avoid circular-slot reuse, range clamping, and overflow handling.

For cleanup, the worker calculates:

```go
safeMinute := currentUnixMinute - 5
```

It processes buckets through `safeMinute` in ascending order. This retains entries for roughly four to five minutes after their exact deadlines before physical removal, ensuring every second in a selected minute is safely expired. `Get`, `Touch`, and `Delete` still treat an entry as expired according to its exact Unix-second deadline. Large buckets and missed minutes are processed in bounded batches.

## Complexity

- `Get`: expected O(1)
- `Set`, `Touch`, and `Delete`: expected O(1)
- Same-key bucket replacement: expected O(1)
- Expiration cleanup: O(m + k) for `m` elapsed minute positions and `k` records processed during catch-up
- Logical resident index: O(n) plus one map entry per occupied minute; Go map backing storage may retain its high-water allocation until `Clear`

The first implementation intentionally excludes memory-capacity eviction, LRU/LFU policy, object-size estimation, and distributed invalidation. See [DESIGN.md](DESIGN.md) for API contracts, lifecycle behavior, concurrency rules, edge cases, and the validation plan.
