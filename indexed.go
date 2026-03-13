package timebox

import "time"

type (
	// Index stores optional projection metadata derived from an event
	Index struct {
		// Status represents the resultant aggregate status. nil means no
		// status change, and "" clears any prior status
		Status *string `json:"status,omitempty"`

		// Labels updates current label values for the aggregate. nil means no
		// label changes, and empty values remove the label
		Labels map[string]string `json:"labels,omitempty"`
	}

	// Indexer derives projection metadata for an event batch
	Indexer func([]*Event) []*Index

	// StatusEntry holds an aggregate ID and the time it entered a status
	StatusEntry struct {
		ID        AggregateID
		Timestamp time.Time
	}
)
