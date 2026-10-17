package raft

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/kode4food/timebox"
)

const (
	stateRootPrefix  = "state/"
	stateMetaPrefix  = stateRootPrefix + "meta/"
	aggRootPrefix    = stateRootPrefix + "agg/"
	statusRootPrefix = stateRootPrefix + "idx/status/"
	tagRootPrefix    = stateRootPrefix + "idx/tag/"
	metaSuffix       = "/meta"
	snapshotSuffix   = "/snapshot"
	eventPrefix      = "/event/"
	seqWidth         = 20
	lastAppliedName  = "last-applied-log"
)

var bucketName = []byte("timebox")

// Key layout:
//   - state/agg/<aggregate>/meta stores aggregate-local metadata
//   - state/agg/<aggregate>/snapshot stores raw Timebox snapshot bytes
//   - state/agg/<aggregate>/event/<seq> stores raw event payloads
//   - state/archive/<stream> stores queued archive records
//   - state/idx/status/<status>/<type>.<time>/<key> stores indexed status
//     timestamps, ordered by type and then status time
//   - state/idx/tag/<tag>/<aggregate> stores tag membership
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

func statusIndexPrefix(status string) []byte {
	return []byte(statusRootPrefix + encodeKeyPart(status) + "/")
}

// statusIndexKey orders entries by type, then status time, so a query bounded
// by both stops at the bound
func statusIndexKey(status, encodedID string, at int64) []byte {
	typ, key, _ := strings.Cut(encodedID, ".")
	return []byte(
		string(statusIndexPrefix(status)) + typ + "." + statusTime(at) +
			"/" + key,
	)
}

// decodeStatusIndexID reads the aggregate from the part of a status key that
// follows its status prefix
func decodeStatusIndexID(rest string) (timebox.AggregateID, error) {
	typ, rest, _ := strings.Cut(rest, ".")
	_, key, _ := strings.Cut(rest, "/")
	return decodeAggregateID(typ + "." + key)
}

// statusTime flips the sign bit so negative times sort before positive ones
func statusTime(at int64) string {
	return fmt.Sprintf("%016x", uint64(at)^(1<<63))
}

func tagIndexPrefix(tag string) []byte {
	return []byte(tagRootPrefix + encodeKeyPart(tag) + "/")
}

func tagIndexKey(tag, encodedID string) []byte {
	return []byte(tagRootPrefix + encodeKeyPart(tag) + "/" + encodedID)
}

func lastAppliedKey() []byte {
	return []byte(stateMetaPrefix + lastAppliedName)
}

func encodeAggregateID(id timebox.AggregateID) string {
	return encodeAggregateType(id.Type) + encodeKeyPart(string(id.Key))
}

// encodeAggregateType encodes the type half of a key, including its trailing
// separator, so it also serves as the scan prefix for a whole type
func encodeAggregateType(typ timebox.ID) string {
	return encodeKeyPart(string(typ)) + "."
}

func decodeAggregateID(value string) (timebox.AggregateID, error) {
	rawType, rawKey, split := strings.Cut(value, ".")
	if !split {
		return timebox.AggregateID{}, timebox.ErrInvalidAggregateID
	}
	typ, err := decodeKeyPart(rawType)
	if err != nil {
		return timebox.AggregateID{}, err
	}
	key, err := decodeKeyPart(rawKey)
	if err != nil {
		return timebox.AggregateID{}, err
	}
	return timebox.NewAggregateID(timebox.ID(typ), timebox.ID(key)), nil
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
