package raft

import (
	"sync"

	"github.com/kode4food/timebox"
)

type (
	publishQueue struct {
		mu     sync.Mutex
		head   *publishNode
		tail   *publishNode
		notify chan struct{}
		n      int
	}

	publishNode struct {
		events []*timebox.Event
		next   *publishNode
	}
)

func newPublishQueue() *publishQueue {
	return &publishQueue{
		notify: make(chan struct{}, 1),
	}
}

func (q *publishQueue) Put(events []*timebox.Event) {
	if len(events) == 0 {
		return
	}
	n := &publishNode{
		events: append([]*timebox.Event(nil), events...),
	}

	q.mu.Lock()
	if q.tail == nil {
		q.head = n
		q.tail = n
	} else {
		q.tail.next = n
		q.tail = n
	}
	q.n++
	q.mu.Unlock()
	q.Signal()
}

func (q *publishQueue) Pop() ([]*timebox.Event, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head == nil {
		return nil, false
	}
	n := q.head
	q.head = n.next
	if q.head == nil {
		q.tail = nil
	}
	q.n--
	return n.events, true
}

func (q *publishQueue) Ready() <-chan struct{} {
	return q.notify
}

func (q *publishQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.n
}

func (q *publishQueue) Signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}
