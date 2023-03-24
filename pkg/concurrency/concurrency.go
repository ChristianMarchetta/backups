package concurrency

import "sync"

// ConcurrencyHandler is used to regulate the number of concurrent tasks
type ConcurrencyHandler struct {
	maxConcurrentTasks int
	runningTasks       int
	cond               sync.Cond
}

// NewConcurrency creates a new Concurrency object
func NewConcurrency(maxConcurrentTasks int) *ConcurrencyHandler {
	mu := sync.Mutex{}
	c := &ConcurrencyHandler{
		maxConcurrentTasks: maxConcurrentTasks,
	}
	c.cond.L = &mu
	return c
}

// NewTask blocks if the maximum number of concurrent tasks is already reached, runs f in a goroutine and returns immediately otherwise.
func (c *ConcurrencyHandler) NewTask(f func()) {
	c.cond.L.Lock()
	for c.runningTasks >= c.maxConcurrentTasks {
		c.cond.Wait()
	}
	defer c.cond.L.Unlock()

	c.runningTasks++
	go func() {
		f()
		c.cond.L.Lock()
		c.runningTasks--
		c.cond.Signal()
		c.cond.L.Unlock()
	}()
}

func (c *ConcurrencyHandler) Wait() {
	c.cond.L.Lock()
	for c.runningTasks > 0 {
		c.cond.Wait()
	}
	c.cond.L.Unlock()
}
