package LCache

var globalCache *Cache

// Init creates the package-level Cache if none exists and returns it.
// Subsequent calls return the same Cache and ignore config until package-level
// Close clears the global instance. Closing the returned *Cache directly does
// not clear the global instance.
// Callers must ensure Init and Close do not run concurrently with each other
// or any other package-level cache operation. Initialize before starting users
// of the global Cache and stop those users before closing it.
func Init(config Config) *Cache {
	if globalCache != nil {
		return globalCache
	} else {
		globalCache = New(config)
		return globalCache
	}
}

// Set stores a value in the package-level Cache. It is a no-op before Init.
func Set(key string, value any, ttlSeconds int64) {
	if globalCache != nil {
		globalCache.Set(key, value, ttlSeconds)
	}
}

// Get reads a value from the package-level Cache. It returns a miss before Init.
func Get(key string) (value any, found bool) {
	if globalCache == nil {
		return nil, false
	}
	return globalCache.Get(key)
}

// GetWithTTL reads a value and its TTL from the package-level Cache. It returns
// a miss before Init.
func GetWithTTL(key string) (value any, expiresAtUnix, remainingTTLSeconds int64, found bool) {
	if globalCache == nil {
		return nil, 0, 0, false
	}
	return globalCache.GetWithTTL(key)
}

// Touch changes the TTL of a value in the package-level Cache. It returns false
// before Init.
func Touch(key string, ttlSeconds int64) bool {
	return globalCache != nil && globalCache.Touch(key, ttlSeconds)
}

// Delete removes a value from the package-level Cache. It returns false before Init.
func Delete(key string) bool {
	return globalCache != nil && globalCache.Delete(key)
}

// Clear removes all values from the package-level Cache. It is a no-op before Init.
func Clear() {
	if globalCache != nil {
		globalCache.Clear()
	}
}

// GlobalStats returns statistics for the package-level Cache. It returns an
// empty snapshot before Init.
func GlobalStats() Stats {
	if globalCache == nil {
		return Stats{}
	}
	return globalCache.Stats()
}

// Close closes and removes the package-level Cache. It is safe to call more
// than once sequentially. A later Init call creates a new Cache. Callers must
// ensure Close does not run concurrently with any package-level cache operation,
// including Init and other Close calls.
func Close() {
	if globalCache == nil {
		return
	}
	globalCache.Close()
	globalCache = nil
}
