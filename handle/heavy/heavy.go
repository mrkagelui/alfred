// Package heavy provides a means to carry out time-consuming, error-prone jobs
package heavy

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// Lifter contains all it needs to carry out the job.
type Lifter struct {
	MaxDelay          time.Duration
	FailurePercentage int
}

// Lift carries out the job.
func (l Lifter) Lift(ctx context.Context, userID string, year, month int) (string, error) {
	if l.MaxDelay == 0 {
		return l.lift(userID, year, month)
	}
	select {
	case <-time.After(rand.N(l.MaxDelay)):
		return l.lift(userID, year, month)
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (l Lifter) lift(userID string, year, month int) (string, error) {
	if rand.IntN(100) < l.FailurePercentage {
		return "", errors.New("unlucky")
	}
	return fmt.Sprintf("success for %v, year %v month %v", userID, year, month), nil
}
