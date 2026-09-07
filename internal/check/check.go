// Package check enforces append contract rules every backend shares
package check

import (
	"fmt"

	"github.com/kode4food/timebox"
)

// Distinct returns timebox.ErrDuplicateAggregate when two requests in one
// append name the same aggregate. Every backend checks expected sequences
// against the state the append started from, so a second request cannot match
func Distinct(reqs []timebox.AppendRequest) error {
	seen := make(map[timebox.AggregateID]struct{}, len(reqs))
	for _, req := range reqs {
		if _, ok := seen[req.ID]; ok {
			return fmt.Errorf(
				"%w: %s", timebox.ErrDuplicateAggregate, req.ID,
			)
		}
		seen[req.ID] = struct{}{}
	}
	return nil
}

// Mutates reports whether a request changes stored state, rather than only
// asserting an expected sequence
func Mutates(req timebox.AppendRequest) bool {
	return len(req.Events) != 0 || req.Status != nil || len(req.Tags) != 0
}
