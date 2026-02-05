package timebox

import (
	"strings"
	"sync"

	"github.com/kode4food/caravan/topic"
)

type (
	// EventHub filters events based on active subscriptions
	EventHub struct {
		inner    topic.Topic[*Event]
		registry *registry
	}

	// Consumer filters events based on interests
	Consumer struct {
		inner     topic.Consumer[*Event]
		interests *interests
		registry  *registry
		filtered  <-chan *Event
		once      sync.Once
		closeOnce sync.Once
	}

	// registry tracks active subscriptions and counts references
	registry struct {
		mu             sync.RWMutex
		subscriptions  map[EventType]map[string]int64
		allEventsCount int64
	}

	// interests describes what events a consumer is interested in
	interests struct {
		eventTypes map[EventType]bool // empty = all event types
		prefix     AggregateID        // nil = all aggregates
	}
)

const aggIDSep = "\x00"

// NewEventHub creates a new EventHub that filters events based on active
// subscriptions
func NewEventHub(inner topic.Topic[*Event]) *EventHub {
	return &EventHub{
		inner: inner,
		registry: &registry{
			subscriptions: make(map[EventType]map[string]int64),
		},
	}
}

// NewConsumer creates a consumer interested in specific event types. If no
// event types are specified, the consumer receives all events
func (eh *EventHub) NewConsumer(eventTypes ...EventType) *Consumer {
	i := &interests{}

	if len(eventTypes) > 0 {
		i.eventTypes = make(map[EventType]bool)
		for _, et := range eventTypes {
			i.eventTypes[et] = true
		}
	}

	eh.registry.register(i)

	return &Consumer{
		inner:     eh.inner.NewConsumer(),
		interests: i,
		registry:  eh.registry,
	}
}

// NewAggregateConsumer creates a consumer interested in events from aggregates
// matching the provided prefix. If no event types are specified, the consumer
// receives all events for aggregates matching the prefix
func (eh *EventHub) NewAggregateConsumer(
	prefix AggregateID, eventTypes ...EventType,
) *Consumer {
	i := &interests{
		prefix: normalizePrefix(prefix),
	}

	if len(eventTypes) > 0 {
		i.eventTypes = make(map[EventType]bool)
		for _, et := range eventTypes {
			i.eventTypes[et] = true
		}
	}

	eh.registry.register(i)

	return &Consumer{
		inner:     eh.inner.NewConsumer(),
		interests: i,
		registry:  eh.registry,
	}
}

// hasSubscribers checks if there are any active subscriptions for an event
func (eh *EventHub) hasSubscribers(
	eventType EventType, aggregateID AggregateID,
) bool {
	return eh.registry.hasSubscribers(eventType, aggregateID)
}

// newProducer returns the underlying producer
func (eh *EventHub) newProducer() topic.Producer[*Event] {
	return eh.inner.NewProducer()
}

// Receive returns a channel of events filtered by the consumer's interests
func (c *Consumer) Receive() <-chan *Event {
	c.once.Do(func() {
		filtered := make(chan *Event, 1)

		go func() {
			defer close(filtered)
			for ev := range c.inner.Receive() {
				if c.matches(ev) {
					filtered <- ev
				}
			}
		}()

		c.filtered = filtered
	})

	return c.filtered
}

// Close unregisters the consumer
func (c *Consumer) Close() error {
	c.closeOnce.Do(func() {
		c.registry.unregister(c.interests)
		c.inner.Close()
	})
	return nil
}

// matches checks if an event matches the consumer's interests
func (c *Consumer) matches(ev *Event) bool {
	if c.interests.prefix != nil &&
		!ev.AggregateID.HasPrefix(c.interests.prefix) {
		return false
	}

	if len(c.interests.eventTypes) > 0 &&
		!c.interests.eventTypes[ev.Type] {
		return false
	}

	return true
}

// register adds a subscription to the registry
func (r *registry) register(i *interests) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if i.prefix == nil && len(i.eventTypes) == 0 {
		r.allEventsCount++
		return
	}

	if len(i.eventTypes) == 0 {
		aggID := aggIDKey(i.prefix)
		if r.subscriptions[""] == nil {
			r.subscriptions[""] = make(map[string]int64)
		}
		r.subscriptions[""][aggID]++
		return
	}

	if i.prefix == nil {
		for et := range i.eventTypes {
			if r.subscriptions[et] == nil {
				r.subscriptions[et] = make(map[string]int64)
			}
			r.subscriptions[et][""]++
		}
		return
	}

	aggID := aggIDKey(i.prefix)
	for et := range i.eventTypes {
		if r.subscriptions[et] == nil {
			r.subscriptions[et] = make(map[string]int64)
		}
		r.subscriptions[et][aggID]++
	}
}

// unregister removes a subscription from the registry
func (r *registry) unregister(i *interests) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if i.prefix == nil && len(i.eventTypes) == 0 {
		r.allEventsCount--
		return
	}

	if len(i.eventTypes) == 0 {
		aggID := aggIDKey(i.prefix)
		r.subscriptions[""][aggID]--
		if r.subscriptions[""][aggID] == 0 {
			delete(r.subscriptions[""], aggID)
		}
		if len(r.subscriptions[""]) == 0 {
			delete(r.subscriptions, "")
		}
		return
	}

	if i.prefix == nil {
		for et := range i.eventTypes {
			r.subscriptions[et][""]--
			if r.subscriptions[et][""] == 0 {
				delete(r.subscriptions[et], "")
			}
			if len(r.subscriptions[et]) == 0 {
				delete(r.subscriptions, et)
			}
		}
		return
	}

	aggID := aggIDKey(i.prefix)
	for et := range i.eventTypes {
		r.subscriptions[et][aggID]--
		if r.subscriptions[et][aggID] == 0 {
			delete(r.subscriptions[et], aggID)
		}
		if len(r.subscriptions[et]) == 0 {
			delete(r.subscriptions, et)
		}
	}
}

// hasSubscribers checks if there are subscriptions for a given event
func (r *registry) hasSubscribers(
	eventType EventType, aggregateID AggregateID,
) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.allEventsCount > 0 {
		return true
	}

	if subs, ok := r.subscriptions[""]; ok {
		aggIDStr := aggIDKey(aggregateID)
		if hasPrefixSubscriber(subs, aggIDStr) {
			return true
		}
	}

	if subs, ok := r.subscriptions[eventType]; ok {
		if _, hasAll := subs[""]; hasAll {
			return true
		}
		aggIDStr := aggIDKey(aggregateID)
		if hasPrefixSubscriber(subs, aggIDStr) {
			return true
		}
	}

	return false
}

func normalizePrefix(prefix AggregateID) AggregateID {
	if len(prefix) == 0 {
		return nil
	}
	return prefix
}

func hasPrefixSubscriber(subs map[string]int64, aggregateID string) bool {
	for prefix := range subs {
		if prefix == "" {
			continue
		}
		if aggregateID == prefix ||
			strings.HasPrefix(aggregateID, prefix+aggIDSep) {
			return true
		}
	}
	return false
}

func aggIDKey(id AggregateID) string {
	return id.Join(aggIDSep)
}
