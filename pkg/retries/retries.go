package retries

import (
	"fmt"
	"log"
	"math"
	"time"
)

func calcWaitTime(i int) time.Duration {
	return time.Duration(math.Pow(2, float64(i))) * time.Millisecond * 100
}

type RateLimiterArgs struct {
	RequestsPerMin int
	MaxRetries     int
}

type RateLimiter struct {
	ticker     *time.Ticker
	maxRetries int
}

func NewRateLimiter(args RateLimiterArgs) *RateLimiter {
	if args.RequestsPerMin <= 0 {
		panic("Request per minute must be greater than 0")
	}

	if args.MaxRetries <= 0 {
		panic("Max retries must be greater than 0")
	}

	return &RateLimiter{
		ticker:     time.NewTicker(time.Duration(60/args.RequestsPerMin) * time.Second),
		maxRetries: args.MaxRetries,
	}
}

func (r *RateLimiter) Retry(f func() error) error {
	<-r.ticker.C
	err := f()
	if err == nil {
		return nil
	}

	for i := 1; i < r.maxRetries; i++ {
		waitTime := calcWaitTime(i)
		log.Printf("Got error, will wait for %s: %v", waitTime, err)
		time.Sleep(waitTime)
		<-r.ticker.C

		err = f()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("max retries reached")
}

// Stop stops the rate limiter
func (r *RateLimiter) Stop() {
	r.ticker.Stop()
}
