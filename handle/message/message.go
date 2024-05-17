// Package message provides a means to send a message for observable reporting
package message

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// Messenger contains necessary handle to send the message.
type Messenger struct {
	MaxDelay          time.Duration
	FailurePercentage int
}

// Message sends the message.
func (m Messenger) Message(ctx context.Context, msgID, userID, result string, year, month int) error {
	if m.MaxDelay == 0 {
		return m.msg()
	}
	select {
	case <-time.After(rand.N(m.MaxDelay)):
		return m.msg()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m Messenger) msg() error {
	if rand.IntN(100) < m.FailurePercentage {
		return errors.New("unlucky")
	}
	return nil
}
