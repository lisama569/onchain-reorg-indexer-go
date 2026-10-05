package query

import "errors"

// errUnavailable is returned by the test memSource for unknown heights.
var errUnavailable = errors.New("memSource: block unavailable")
