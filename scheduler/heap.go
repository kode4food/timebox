package scheduler

import (
	"container/heap"
	"time"

	"github.com/kode4food/timebox"
)

type (
	scheduleHeap struct {
		byKey map[timebox.ScheduleKey]*heapItem
		items []*heapItem
	}

	heapItem struct {
		schedule *timebox.Schedule
		readyAt  time.Time
		index    int
	}
)

func (h *scheduleHeap) Replace(schedule *timebox.Schedule) {
	if item, ok := h.byKey[schedule.Key]; ok {
		item.schedule = schedule
		item.readyAt = schedule.At
		heap.Fix(h, item.index)
		return
	}
	heap.Push(h, &heapItem{schedule: schedule, readyAt: schedule.At})
}

func (h *scheduleHeap) Remove(key timebox.ScheduleKey) {
	if item, ok := h.byKey[key]; ok {
		heap.Remove(h, item.index)
	}
}

func (h *scheduleHeap) Peek() *heapItem {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

func (h *scheduleHeap) Len() int {
	return len(h.items)
}

func (h *scheduleHeap) Less(i, j int) bool {
	l := h.items[i]
	r := h.items[j]
	if l.readyAt.Equal(r.readyAt) {
		return l.schedule.Key < r.schedule.Key
	}
	return l.readyAt.Before(r.readyAt)
}

func (h *scheduleHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}

func (h *scheduleHeap) Push(v any) {
	item := v.(*heapItem)
	item.index = len(h.items)
	h.items = append(h.items, item)
	h.byKey[item.schedule.Key] = item
}

func (h *scheduleHeap) Pop() any {
	last := len(h.items) - 1
	item := h.items[last]
	h.items[last] = nil
	h.items = h.items[:last]
	delete(h.byKey, item.schedule.Key)
	return item
}

func newScheduleHeap() *scheduleHeap {
	return &scheduleHeap{
		byKey: map[timebox.ScheduleKey]*heapItem{},
	}
}
