package raft

import (
	"encoding/base64"
	"strings"

	"github.com/kode4food/timebox"
)

const (
	stateRootPrefix   = "state/"
	stateMetaPrefix   = stateRootPrefix + "meta/"
	aggRootPrefix     = stateRootPrefix + "agg/"
	archiveRootPrefix = stateRootPrefix + "archive/"
	statusRootPrefix  = stateRootPrefix + "idx/status/"
	labelRootPrefix   = stateRootPrefix + "idx/label/"
	labelValsPrefix   = stateRootPrefix + "idx/label-values/"
	metaSuffix        = "/meta"
	snapshotSuffix    = "/snapshot"
	eventPrefix       = "/event/"
	seqWidth          = 20
	lastAppliedName   = "last-applied-log"
)

var bucketName = []byte("timebox")

// Key layout:
//   - state/agg/<aggregate>/meta stores aggregate-local metadata
//   - state/agg/<aggregate>/snapshot stores raw Timebox snapshot bytes
//   - state/agg/<aggregate>/event/<seq> stores raw event payloads
//   - state/archive/<stream> stores queued archive records
//   - state/idx/status/<status>/<aggregate> stores indexed status timestamps
//   - state/idx/label/<label>/<value>/<aggregate> stores label membership
//   - state/idx/label-values/<label>/<value> stores distinct label values
//
// Variable key parts are base64url encoded so iteration remains
// lexicographically well-structured without introducing ambiguous separators

// AggregateMetaPrefix returns the key prefix for aggregate metadata
func AggregateMetaPrefix() []byte {
	return []byte(aggRootPrefix)
}

// AggregateMetaKey returns the metadata key for one encoded aggregate ID
func AggregateMetaKey(encodedID string) []byte {
	return []byte(aggRootPrefix + encodedID + metaSuffix)
}

func aggregateSnapshotKey(encodedID string) []byte {
	return []byte(aggRootPrefix + encodedID + snapshotSuffix)
}

func aggregateEventPrefix(encodedID string) []byte {
	return []byte(aggRootPrefix + encodedID + eventPrefix)
}

func aggregateEventKeyFromPrefix(prefix []byte, seq int64) []byte {
	key := make([]byte, len(prefix)+seqWidth)
	copy(key, prefix)
	writeSequence(key[len(prefix):], seq)
	return key
}

func archivePrefix() []byte {
	return []byte(archiveRootPrefix)
}

func archiveRecordKey(streamID string) []byte {
	return []byte(archiveRootPrefix + streamID)
}

func statusIndexPrefix(status string) []byte {
	return []byte(statusRootPrefix + encodeKeyPart(status) + "/")
}

func statusIndexKey(status, encodedID string) []byte {
	return []byte(
		statusRootPrefix + encodeKeyPart(status) + "/" + encodedID,
	)
}

func labelIndexPrefix(label, value string) []byte {
	return []byte(
		labelRootPrefix + encodeKeyPart(label) + "/" + encodeKeyPart(value) +
			"/",
	)
}

func labelIndexKey(label, value, encodedID string) []byte {
	return []byte(
		labelRootPrefix + encodeKeyPart(label) + "/" + encodeKeyPart(value) +
			"/" + encodedID,
	)
}

func labelValuesPrefix(label string) []byte {
	return []byte(labelValsPrefix + encodeKeyPart(label) + "/")
}

func labelValueKey(label, value string) []byte {
	return []byte(
		labelValsPrefix + encodeKeyPart(label) + "/" + encodeKeyPart(value),
	)
}

func lastAppliedKey() []byte {
	return []byte(stateMetaPrefix + lastAppliedName)
}

func encodeAggregateID(id timebox.AggregateID) string {
	if len(id) == 0 {
		return "_"
	}
	parts := make([]string, len(id))
	for i, part := range id {
		parts[i] = encodeKeyPart(string(part))
	}
	return strings.Join(parts, ".")
}

func decodeAggregateID(value string) (timebox.AggregateID, error) {
	if value == "_" || value == "" {
		return timebox.AggregateID{}, nil
	}

	rawParts := strings.Split(value, ".")
	res := make(timebox.AggregateID, len(rawParts))
	for i, part := range rawParts {
		decoded, err := decodeKeyPart(part)
		if err != nil {
			return nil, err
		}
		res[i] = timebox.ID(decoded)
	}
	return res, nil
}

func encodeKeyPart(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeKeyPart(value string) (string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}

func writeSequence(dst []byte, seq int64) {
	if seq < 0 {
		seq = 0
	}
	for i := len(dst) - 1; i >= 0; i-- {
		dst[i] = byte('0' + seq%10)
		seq /= 10
	}
}
