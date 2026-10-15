package timebox

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"
)

type (
	// QueueHandler processes one dequeued message. The message is only consumed
	// if the handler returns nil. Any error leaves it in place for a later
	// Consume
	QueueHandler func(*Transaction, *Message) error

	queueState struct {
		Messages []*Message `json:"messages,omitempty"`
	}
)

const (
	queueAggregateType ID        = "_tb.queue_"
	queueItemAdded     EventType = "_tb.queue.added"
	queueItemConsumed  EventType = "_tb.queue.consumed"

	// consumePoll bounds how long Consume waits between wake notifications
	// before rechecking a queue for messages from other processes
	consumePoll = time.Second
)

var (
	// ErrQueueHandlerRequired indicates a Consume call is missing a handler
	ErrQueueHandlerRequired = errors.New("queue handler is required")

	// ErrQueueMessageRequired indicates a queue has no published message
	ErrQueueMessageRequired = errors.New("queue message is required")

	// ErrQueueMessageTypeRequired indicates an empty published message type
	ErrQueueMessageTypeRequired = errors.New(
		"queue message type is required",
	)

	// errQueueEmpty signals consumeOnce found nothing to consume. Consume
	// retries on it instead of returning it, so it never reaches a caller
	errQueueEmpty = errors.New("queue is empty")
)

// Publish appends a durable message to the named queue
func (t *Transaction) Publish(queue ID, message *Message) error {
	if err := validateQueueMessage(message); err != nil {
		return err
	}
	id := makeQueueID(queue)
	_, err := t.execInternal(t.store.queue, id,
		func(_ queueState, ag *Aggregator[queueState]) error {
			return ag.Raise(queueItemAdded, message)
		},
	)
	return err
}

// Consume blocks until the oldest unconsumed message of a queue is handled or
// ctx is done. A message is removed only if handler returns nil. Any other
// error stops Consume and leaves the message in place
func (s *Store) Consume(
	ctx context.Context, queue ID, handler QueueHandler,
) error {
	if handler == nil {
		return ErrQueueHandlerRequired
	}
	for {
		err := s.consumeOnce(queue, handler)
		if !errors.Is(err, errQueueEmpty) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.queueWake:
		case <-time.After(consumePoll):
		}
	}
}

func (s *Store) consumeOnce(queue ID, handler QueueHandler) error {
	id := makeQueueID(queue)
	s.queue.invalidate(id)
	return s.Transact(func(t *Transaction) error {
		_, err := t.execInternal(s.queue, id,
			func(st queueState, ag *Aggregator[queueState]) error {
				if len(st.Messages) == 0 {
					return errQueueEmpty
				}
				if err := handler(t, st.Messages[0]); err != nil {
					return err
				}
				return ag.Raise(queueItemConsumed, Empty{})
			},
		)
		return err
	})
}

func (s *Store) notifyQueueChange(_ queueState, evs []*Event) {
	if len(evs) == 0 {
		return
	}
	select {
	case s.queueWake <- struct{}{}:
	default:
	}
}

func newQueueState() queueState {
	return queueState{}
}

func reduceQueue(st queueState, evs []*Event) queueState {
	if len(evs) == 0 {
		return st
	}
	messages := slices.Clone(st.Messages)
	for _, ev := range evs {
		switch ev.Type {
		case queueItemAdded:
			message, err := ev.GetValue[*Message]()
			if err == nil {
				messages = append(messages, message)
			}
		case queueItemConsumed:
			if _, err := ev.GetValue[Empty](); err == nil &&
				len(messages) > 0 {
				messages[0] = nil
				messages = messages[1:]
			}
		}
	}
	st.Messages = messages
	return st
}

func makeQueueID(queue ID) AggregateID {
	return NewAggregateID(queueAggregateType, queue)
}

func validateQueueMessage(message *Message) error {
	if message == nil {
		return ErrQueueMessageRequired
	}
	if message.Type == "" {
		return ErrQueueMessageTypeRequired
	}
	if message.AggregateID.Type == "" || message.AggregateID.Key == "" {
		return fmt.Errorf("%w: queue message", ErrInvalidAggregateID)
	}
	return nil
}
