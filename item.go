package LCache

import (
	"fmt"
	"strings"
)

// Item holds a value and its generated cache key and statistics tag.
// Its zero value has a nil value and an empty key and tag.
// Callers must synchronize mutations via Attr or Val with concurrent use of the Item.
type Item struct {
	value any
	tag   string
	key   string
}

// NewItem constructs an Item by joining ordered tag segments with colons.
// The tag and key initially match; both are empty when no segments are supplied.
// Use Attr to append key attributes and Val to set the initially nil value.
func NewItem(tagParts ...string) *Item {
	tag := strings.Join(tagParts, ":")
	return &Item{tag: tag, key: tag}
}

// Attr appends name to the tag and name:value to the key, then returns item.
// Values use fmt.Sprint formatting and are captured on each call. Colons are
// not escaped, and types are not encoded; callers must avoid ambiguous strings.
// Calls preserve their order, and repeated names are appended rather than replaced.
// Attr does not change the value to cache or any previously stored entry.
func (item *Item) Attr(name string, value any) *Item {
	item.tag += ":" + name
	item.key += ":" + name + ":" + fmt.Sprint(value)
	return item
}

// Val sets the value to cache and returns item for chaining.
// The value is retained directly, not cloned. Val does not change the key or tag.
// Callers must synchronize Val with Set and other Val calls on the same Item.
func (item *Item) Val(value any) *Item {
	item.value = value
	return item
}

// GetTag returns the base tag followed by colon-separated attribute names.
func (item *Item) GetTag() string {
	return item.tag
}

// GetKey returns the base tag followed by colon-separated attribute names and values.
func (item *Item) GetKey() string {
	return item.key
}
