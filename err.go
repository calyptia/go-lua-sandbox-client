package luasandbox

import "fmt"

// IndexedError holds information about the specific record index that failed
// to be processed by Lua.
type IndexedError struct {
	Index uint
	Err   error
}

func (e IndexedError) Error() string {
	return fmt.Sprintf("%d: %v", e.Index, e.Err)
}
