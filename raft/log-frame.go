package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"slices"

	"go.etcd.io/raft/v3/raftpb"
)

const (
	frameMaxBody = 256 * 1024 * 1024

	frameTypeHardState = 0xFF

	idxRecordSize = 16
)

func appendFrame(dst []byte, ent raftpb.Entry) ([]byte, error) {
	if len(ent.Data) > frameMaxBody {
		return dst, ErrCorruptState
	}
	bodyLen := 1 + 8 + 8 + len(ent.Data)
	frameLen := 4 + bodyLen + 4
	dst = slices.Grow(dst, frameLen)
	start := len(dst)
	dst = dst[:start+frameLen]
	frame := dst[start:]
	binary.BigEndian.PutUint32(frame[:4], uint32(bodyLen))
	frame[4] = byte(ent.Type)
	binary.BigEndian.PutUint64(frame[5:13], ent.Index)
	binary.BigEndian.PutUint64(frame[13:21], ent.Term)
	copy(frame[21:21+len(ent.Data)], ent.Data)
	sum := crc32.ChecksumIEEE(frame[4 : 4+bodyLen])
	binary.BigEndian.PutUint32(frame[4+bodyLen:], sum)
	return dst, nil
}

func appendHardStateFrame(dst []byte, hs raftpb.HardState) []byte {
	const bodyLen = 1 + 8 + 8 + 8
	const frameLen = 4 + bodyLen + 4
	dst = slices.Grow(dst, frameLen)
	start := len(dst)
	dst = dst[:start+frameLen]
	frame := dst[start:]
	binary.BigEndian.PutUint32(frame[:4], bodyLen)
	frame[4] = frameTypeHardState
	binary.BigEndian.PutUint64(frame[5:13], hs.Commit)
	binary.BigEndian.PutUint64(frame[13:21], hs.Term)
	binary.BigEndian.PutUint64(frame[21:29], hs.Vote)
	sum := crc32.ChecksumIEEE(frame[4 : 4+bodyLen])
	binary.BigEndian.PutUint32(frame[4+bodyLen:], sum)
	return dst
}

func appendIdxRecord(dst []byte, pt logPoint) []byte {
	dst = slices.Grow(dst, idxRecordSize)
	start := len(dst)
	dst = dst[:start+idxRecordSize]
	binary.BigEndian.PutUint64(dst[start:start+8], pt.idx)
	binary.BigEndian.PutUint64(dst[start+8:start+16], uint64(pt.off))
	return dst
}

func readLogFrame(r *bufio.Reader) (raftpb.Entry, int64, error) {
	for {
		ent, _, n, err := readWALFrame(r)
		if err != nil {
			return raftpb.Entry{}, 0, err
		}
		if ent != nil {
			return *ent, n, nil
		}
	}
}

// readWALFrame returns either an entry or a HardState, plus the frame size.
// Exactly one of the two pointers is non-nil on success
func readWALFrame(r *bufio.Reader) (
	*raftpb.Entry, *raftpb.HardState, int64, error,
) {
	var hdr [4]byte
	n, err := io.ReadFull(r, hdr[:])
	switch {
	case err == nil:
	case errors.Is(err, io.EOF) && n == 0:
		return nil, nil, 0, io.EOF
	default:
		return nil, nil, 0, ErrCorruptState
	}

	bodyLen := binary.BigEndian.Uint32(hdr[:])
	switch {
	case bodyLen < 17:
		return nil, nil, 0, ErrCorruptState
	case bodyLen > frameMaxBody:
		return nil, nil, 0, ErrCorruptState
	}

	body := make([]byte, int(bodyLen)+4)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, nil, 0, ErrCorruptState
	}
	want := binary.BigEndian.Uint32(body[len(body)-4:])
	if crc32.ChecksumIEEE(body[:len(body)-4]) != want {
		return nil, nil, 0, ErrCorruptState
	}

	frameSize := int64(4 + len(body))
	if body[0] == frameTypeHardState {
		if bodyLen < 25 {
			return nil, nil, 0, ErrCorruptState
		}
		hs := &raftpb.HardState{
			Commit: binary.BigEndian.Uint64(body[1:9]),
			Term:   binary.BigEndian.Uint64(body[9:17]),
			Vote:   binary.BigEndian.Uint64(body[17:25]),
		}
		return nil, hs, frameSize, nil
	}

	ent := &raftpb.Entry{
		Type:  raftpb.EntryType(body[0]),
		Index: binary.BigEndian.Uint64(body[1:9]),
		Term:  binary.BigEndian.Uint64(body[9:17]),
		Data:  append([]byte(nil), body[17:len(body)-4]...),
	}
	return ent, nil, frameSize, nil
}

func writeIdxPoint(f *os.File, pt logPoint) error {
	var buf [idxRecordSize]byte
	binary.BigEndian.PutUint64(buf[:8], pt.idx)
	binary.BigEndian.PutUint64(buf[8:16], uint64(pt.off))
	_, err := f.Write(buf[:])
	return err
}

func rewriteIdx(path string, pts []logPoint) error {
	f, err := os.OpenFile(path,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600,
	)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	for _, pt := range pts {
		if err := writeIdxPoint(f, pt); err != nil {
			return err
		}
	}
	return f.Sync()
}
