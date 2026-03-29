package raft

import "go.etcd.io/raft/v3/raftpb"

type tailCache struct {
	buf   []raftpb.Entry
	head  int
	n     int
	first uint64
}

func newTailCache(n int) tailCache {
	return tailCache{
		buf: make([]raftpb.Entry, n),
	}
}

func (c *tailCache) entries(lo, hi, maxSize uint64) ([]raftpb.Entry, bool) {
	if !c.has(lo) {
		return nil, false
	}
	last := c.first + uint64(c.n) - 1
	if hi != 0 && hi-1 > last {
		return nil, false
	}

	ents := make([]raftpb.Entry, 0, int(hi-lo))
	var total uint64
	pos := (c.head + int(lo-c.first)) % len(c.buf)
	for idx := lo; idx < hi; idx++ {
		ent := c.buf[pos]
		if len(ents) != 0 && total+uint64(ent.Size()) > maxSize {
			break
		}
		ents = append(ents, cloneEntry(ent))
		total += uint64(ent.Size())
		pos++
		if pos == len(c.buf) {
			pos = 0
		}
	}
	return ents[:len(ents):len(ents)], true
}

func (c *tailCache) term(idx uint64) (uint64, bool) {
	if !c.has(idx) {
		return 0, false
	}
	return c.at(idx).Term, true
}

func (c *tailCache) put(ent raftpb.Entry) {
	if len(c.buf) == 0 {
		return
	}
	if c.n == 0 {
		c.first = ent.Index
		c.n = 1
		c.buf[0] = cloneEntry(ent)
		return
	}

	last := c.first + uint64(c.n) - 1
	if ent.Index != last+1 {
		c.reset()
		c.put(ent)
		return
	}
	if c.n == len(c.buf) {
		c.buf[c.head] = cloneEntry(ent)
		c.head = (c.head + 1) % len(c.buf)
		c.first++
		return
	}
	pos := (c.head + c.n) % len(c.buf)
	c.buf[pos] = cloneEntry(ent)
	c.n++
}

func (c *tailCache) truncate(first uint64) {
	if c.n == 0 {
		return
	}
	if first <= c.first {
		c.reset()
		return
	}

	last := c.first + uint64(c.n) - 1
	if first > last {
		return
	}
	c.n = int(first - c.first)
}

func (c *tailCache) reset() {
	c.head = 0
	c.n = 0
	c.first = 0
}

func (c *tailCache) has(idx uint64) bool {
	if c.n == 0 {
		return false
	}
	last := c.first + uint64(c.n) - 1
	return idx >= c.first && idx <= last
}

func (c *tailCache) at(idx uint64) raftpb.Entry {
	pos := (c.head + int(idx-c.first)) % len(c.buf)
	return c.buf[pos]
}

func cloneEntry(ent raftpb.Entry) raftpb.Entry {
	cp := ent
	cp.Data = append([]byte(nil), ent.Data...)
	return cp
}
