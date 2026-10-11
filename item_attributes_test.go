package LCache_test

import (
	"testing"
	"time"

	"github.com/LazyEasyDev/LCache"
)

func TestNewItemTagParts(test *testing.T) {
	for _, testCase := range []struct {
		name     string
		tagParts []string
		want     string
	}{
		{name: "no segments", want: ""},
		{name: "one segment", tagParts: []string{"users"}, want: "users"},
		{name: "multiple segments", tagParts: []string{"users", "name", "display"}, want: "users:name:display"},
		{name: "empty segment", tagParts: []string{"users", "", "name"}, want: "users::name"},
		{name: "literal colon", tagParts: []string{"users:active", "name"}, want: "users:active:name"},
	} {
		test.Run(testCase.name, func(test *testing.T) {
			item := LCache.NewItem(testCase.tagParts...)
			if item.GetTag() != testCase.want || item.GetKey() != testCase.want {
				test.Errorf("NewItem() has tag %q and key %q; want %q", item.GetTag(), item.GetKey(), testCase.want)
			}
		})
	}
}

func TestItemAttr(test *testing.T) {
	item := LCache.NewItem("users", "name")
	if got := item.Attr("id", 42).Attr("active", true); got != item {
		test.Fatal("Attr did not return the original Item pointer")
	}
	if got := item.GetTag(); got != "users:name:id:active" {
		test.Errorf("GetTag() = %q; want users:name:id:active", got)
	}
	if got := item.GetKey(); got != "users:name:id:42:active:true" {
		test.Errorf("GetKey() = %q; want users:name:id:42:active:true", got)
	}
	other := LCache.NewItem("users", "name").Attr("id", 43).Attr("active", false)
	if other.GetTag() != item.GetTag() || other.GetKey() == item.GetKey() {
		test.Errorf("different attribute values have tag %q and key %q; want same tag and different key", other.GetTag(), other.GetKey())
	}
}

func TestItemAttrFormatting(test *testing.T) {
	identifiers := []int{42}
	item := LCache.NewItem("items").
		Attr("ids", identifiers).
		Attr("optional", nil).
		Attr("timeout", time.Minute).
		Attr("ratio", 1.5).
		Attr("role", "role:admin")
	identifiers[0] = 99
	if got := item.GetTag(); got != "items:ids:optional:timeout:ratio:role" {
		test.Errorf("GetTag() = %q; want items:ids:optional:timeout:ratio:role", got)
	}
	if got := item.GetKey(); got != "items:ids:[42]:optional:<nil>:timeout:1m0s:ratio:1.5:role:role:admin" {
		test.Errorf("GetKey() = %q; want captured and formatted attribute values", got)
	}
	duplicate := LCache.NewItem("users").Attr("id", 42).Attr("id", 43)
	if duplicate.GetTag() != "users:id:id" || duplicate.GetKey() != "users:id:42:id:43" {
		test.Errorf("duplicate attributes have tag %q and key %q; want both attributes in order", duplicate.GetTag(), duplicate.GetKey())
	}
	empty := LCache.NewItem().Attr("", "")
	if empty.GetTag() != ":" || empty.GetKey() != "::" {
		test.Errorf("empty attribute has tag %q and key %q; want literal delimiters", empty.GetTag(), empty.GetKey())
	}
}

func TestItemAttrStoredEntry(test *testing.T) {
	cache := LCache.New(LCache.DefaultConfig())
	test.Cleanup(cache.Close)
	item := LCache.NewItem("users", "name").Val("Alice").Attr("id", 42)
	originalKey, originalTag := item.GetKey(), item.GetTag()
	cache.Set(item, 60)
	item.Attr("active", true)
	if value, found := cache.Get(originalKey); !found || value != "Alice" {
		test.Fatalf("Get(original key) = %v, %v; want Alice, true", value, found)
	}
	if _, found := cache.Get(item.GetKey()); found {
		test.Fatal("Attr moved the stored entry to the new key")
	}
	if stats := cache.Stats(); stats.Total != 1 || stats.Count(originalTag) != 1 || stats.Count(item.GetTag()) != 0 {
		test.Fatalf("Stats() after Attr = %+v; want unchanged original entry", stats)
	}
	cache.Set(item, 60)
	if value, found := cache.Get(item.GetKey()); !found || value != "Alice" {
		test.Fatalf("Get(new key) = %v, %v; want Alice, true", value, found)
	}
	if stats := cache.Stats(); stats.Total != 2 || stats.Count(originalTag) != 1 || stats.Count(item.GetTag()) != 1 {
		test.Errorf("Stats() after Set = %+v; want entries under both keys and tags", stats)
	}
}
