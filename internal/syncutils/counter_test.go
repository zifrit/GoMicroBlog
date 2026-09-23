package syncutils

import (
	"sync"
	"testing"
)

func TestCounterReturnsUniqueSequentialValuesWithConcurrentCallers(t *testing.T) {
	const callers = 100
	var counter Counter
	values := make(chan int64, callers)
	var group sync.WaitGroup

	for i := 0; i < callers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			values <- counter.Next()
		}()
	}
	group.Wait()
	close(values)

	seen := make(map[int64]bool, callers)
	for value := range values {
		seen[value] = true
	}
	if len(seen) != callers {
		t.Fatalf("expected %d unique values, got %d", callers, len(seen))
	}
}
