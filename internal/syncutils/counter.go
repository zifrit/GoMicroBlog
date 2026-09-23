package syncutils

import "sync/atomic"

type Counter struct {
	value atomic.Int64
}

func (c *Counter) Next() int64 {
	return c.value.Add(1)
}
