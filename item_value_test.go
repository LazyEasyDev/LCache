package LCache_test

import (
	"reflect"
	"testing"

	"github.com/LazyEasyDev/LCache"
)

func TestItemVal(test *testing.T) {
	cache := LCache.New(LCache.DefaultConfig())
	test.Cleanup(cache.Close)
	item := LCache.NewItem("users").Attr("id", 42)
	if got := item.Val("Alice"); got != item {
		test.Fatal("Val did not return the original Item pointer")
	}
	if item.GetTag() != "users:id" || item.GetKey() != "users:id:42" {
		test.Fatalf("Val changed identity to tag %q and key %q", item.GetTag(), item.GetKey())
	}
	cache.Set(item, 60)
	item.Val("Bob")
	if value, found := cache.Get(item.GetKey()); !found || value != "Alice" {
		test.Fatalf("Get() after Val = %v, %v; want Alice, true until the next Set", value, found)
	}
	cache.Set(item, 60)
	if value, found := cache.Get(item.GetKey()); !found || value != "Bob" {
		test.Fatalf("Get() after Set = %v, %v; want Bob, true", value, found)
	}
	cache.Set(item.Val(nil), 60)
	if value, found := cache.Get(item.GetKey()); !found || value != nil {
		test.Fatalf("Get() after Val(nil) = %v, %v; want nil, true", value, found)
	}
	if stats := cache.Stats(); stats.Total != 1 || stats.Count(item.GetTag()) != 1 {
		test.Errorf("Stats() = %+v; want one entry under %q", stats, item.GetTag())
	}
}

func TestItemWithoutVal(test *testing.T) {
	cache := LCache.New(LCache.DefaultConfig())
	test.Cleanup(cache.Close)
	item := LCache.NewItem("users")
	if item.GetTag() != "users" || item.GetKey() != "users" {
		test.Fatalf("NewItem without attributes has tag %q and key %q; want users", item.GetTag(), item.GetKey())
	}
	cache.Set(item, 60)
	if value, found := cache.Get(item.GetKey()); !found || value != nil {
		test.Fatalf("Get() without Val = %v, %v; want nil, true", value, found)
	}
	cache.Set(&LCache.Item{}, 60)
	if value, found := cache.Get(""); !found || value != nil {
		test.Errorf("Get(zero Item key) = %v, %v; want nil, true", value, found)
	}
}

func TestItemValValueTypes(test *testing.T) {
	cache := LCache.New(LCache.DefaultConfig())
	test.Cleanup(cache.Close)
	var typedNil *int
	valuePointer := new(int)
	for _, testCase := range []struct {
		name  string
		value any
	}{
		{name: "nil", value: nil},
		{name: "typed nil", value: typedNil},
		{name: "integer", value: 42},
		{name: "pointer", value: valuePointer},
		{name: "slice", value: []int{1, 2}},
		{name: "map", value: map[string]any{"id": 42}},
	} {
		test.Run(testCase.name, func(test *testing.T) {
			item := LCache.NewItem("values").Attr("kind", testCase.name).Val(testCase.value)
			cache.Set(item, 60)
			value, found := cache.Get(item.GetKey())
			if !found || !reflect.DeepEqual(value, testCase.value) {
				test.Fatalf("Get() = %#v, %v; want %#v, true", value, found, testCase.value)
			}
			if testCase.name == "pointer" && value != valuePointer {
				test.Error("Val did not retain the original value pointer")
			}
		})
	}
}

func TestGlobalSetItemVal(test *testing.T) {
	LCache.Close()
	test.Cleanup(LCache.Close)
	item := LCache.NewItem("users", "name").Attr("id", 42).Attr("active", true).Val("Alice")
	LCache.Set(item, 60)
	if value, found := LCache.Get(item.GetKey()); found || value != nil {
		test.Fatalf("Get() before Init = %v, %v; want nil, false", value, found)
	}
	LCache.Init(LCache.DefaultConfig())
	LCache.Set(item, 60)
	if value, found := LCache.Get("users:name:id:42:active:true"); !found || value != "Alice" {
		test.Fatalf("Get() after Set = %v, %v; want Alice, true", value, found)
	}
	if stats := LCache.GlobalStats(); stats.Total != 1 || stats.Count("users:name:id:active") != 1 {
		test.Errorf("GlobalStats() = %+v; want one entry under users:name:id:active", stats)
	}
}
