package dbos

import (
	"maps"
	"slices"
	"time"

	"github.com/kode4food/timebox"
)

type (
	projection struct {
		streams         map[timebox.AggregateID]*stream
		archives        []string
		archiveSnapshot []string
	}

	stream struct {
		events   []eventRecord
		snapshot []byte
		statusAt time.Time
		tags     map[string]bool
		status   string
		base     int64
		next     int64
		snapSeq  int64
		live     bool
	}
)

func (p *projection) apply(e *journalEntry) {
	req := e.Mutation
	switch {
	case req.Archive != nil:
		p.applyArchive(*req.Archive, e.ArchiveID)
	case req.Snapshot != nil:
		p.applySnapshot(req.Snapshot)
	default:
		for i := range req.Appends {
			p.applyAppend(&req.Appends[i])
		}
	}
}

func (p *projection) applyAppend(req *appendRecord) {
	next := req.Expected + int64(len(req.Events))
	if req.ID == timebox.ArchiveQueueID {
		p.archives = updateArchiveRefs(p.archives, req.Events)
		if req.Delete {
			p.archives = nil
			p.archiveSnapshot = nil
		}
	}
	if req.Delete {
		p.streams[req.ID] = &stream{
			base:    next,
			next:    next,
			snapSeq: next,
		}
		return
	}
	if !req.mutates() {
		return
	}

	s := p.stream(req.ID)
	s.live = true
	s.next = next
	s.events = append(s.events, req.Events...)
	if req.Status != nil {
		s.status = *req.Status
		s.statusAt = req.StatusAt
	}
	maps.Copy(s.tags, req.Tags)
	for tag, add := range req.Tags {
		if !add {
			delete(s.tags, tag)
		}
	}
}

func (p *projection) applySnapshot(req *timebox.SnapshotRequest) {
	s := p.stream(req.ID)
	if req.ID == timebox.ArchiveQueueID {
		refs := slices.Clone(p.archiveSnapshot)
		for _, ev := range s.from(s.snapSeq) {
			if ev.Sequence >= req.Sequence {
				break
			}
			refs = updateArchiveRefs(refs, []eventRecord{ev})
		}
		p.archiveSnapshot = refs
	}
	s.snapshot = req.Data
	s.snapSeq = req.Sequence
	s.live = true
	if req.TrimEvents {
		s.base = max(s.base, min(req.Sequence, s.next))
		s.events = slices.Clone(s.from(s.base))
	}
}

func (p *projection) checkAppends(reqs []appendRecord) *conflict {
	for _, req := range reqs {
		var seq int64
		var events []eventRecord
		if s := p.streams[req.ID]; s != nil {
			seq = max(s.next, s.snapSeq)
			events = s.from(req.Expected)
		}
		if seq != req.Expected {
			return &conflict{
				ID:       req.ID,
				Expected: req.Expected,
				Actual:   seq,
				Events:   events,
			}
		}
	}
	return nil
}

func (p *projection) stream(id timebox.AggregateID) *stream {
	if s := p.streams[id]; s != nil {
		if s.tags == nil {
			s.tags = map[string]bool{}
		}
		return s
	}
	s := &stream{tags: map[string]bool{}}
	p.streams[id] = s
	return s
}

func (p *projection) applyArchive(id timebox.AggregateID, workflowID string) {
	p.addArchive(workflowID)
	delete(p.streams, id)
}

func (p *projection) addArchive(workflowID string) {
	queue := p.stream(timebox.ArchiveQueueID)
	seq := max(queue.next, queue.snapSeq)
	queue.events = append(queue.events, eventRecord{
		ArchiveID: workflowID,
		Sequence:  seq,
	})
	queue.next = seq + 1
	queue.live = true
	p.archives = append(p.archives, workflowID)
}

func (s *stream) from(seq int64) []eventRecord {
	pos, _ := slices.BinarySearchFunc(s.events, seq, compareSequence)
	return s.events[pos:]
}

func compareSequence(ev eventRecord, seq int64) int {
	switch {
	case ev.Sequence < seq:
		return -1
	case ev.Sequence > seq:
		return 1
	default:
		return 0
	}
}

func updateArchiveRefs(refs []string, events []eventRecord) []string {
	for _, ev := range events {
		switch {
		case ev.ArchiveID != "":
			refs = append(refs, ev.ArchiveID)
		case ev.Type == queueItemConsumed && len(refs) > 0:
			refs[0] = ""
			refs = refs[1:]
		}
	}
	return refs
}
