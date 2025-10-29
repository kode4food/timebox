package timebox

import (
	"github.com/kode4food/caravan"
	"github.com/kode4food/caravan/topic"
)

type EventHub topic.Topic[*Event]

func NewEventHub() EventHub {
	return caravan.NewTopic[*Event]()
}
