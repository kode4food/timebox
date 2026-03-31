package raft

import (
	"bufio"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"slices"

	"go.etcd.io/raft/v3/raftpb"

	bin "github.com/kode4food/timebox/internal/binary"
)

const (
	frameMaxBody = 256 * 1024 * 1024

	frameTypeHardState = 0xFF

	frameHeaderSize = 4
	frameCRCLen     = 4
	frameTypeLen    = 1
	frameU64Len     = 8

	frameEntryMetaLen = frameTypeLen + 2*frameU64Len
	frameHardStateLen = frameTypeLen + 3*frameU64Len

	idxRecordSize = 16
)

func appendFrame(dst []byte, ent raftpb.Entry) ([]byte, error) {
	if len(ent.Data) > frameMaxBody {
		return dst, bin.ErrCorruptState
	}
	bodyLen := frameEntryMetaLen + len(ent.Data)
	dst = slices.Grow(dst, frameHeaderSize+bodyLen+frameCRCLen)
	dst = bin.AppendUint32(dst, uint32(bodyLen))
	bodyStart := len(dst)
	dst = bin.AppendByte(dst, byte(ent.Type))
	dst = bin.AppendUint64(dst, ent.Index)
	dst = bin.AppendUint64(dst, ent.Term)
	dst = append(dst, ent.Data...)
	body := dst[bodyStart:]
	sum := crc32.ChecksumIEEE(body)
	dst = bin.AppendUint32(dst, sum)
	return dst, nil
}

func appendHardStateFrame(dst []byte, hs raftpb.HardState) []byte {
	const bodyLen = frameHardStateLen

	dst = slices.Grow(dst, frameHeaderSize+bodyLen+frameCRCLen)
	dst = bin.AppendUint32(dst, bodyLen)
	bodyStart := len(dst)
	dst = bin.AppendByte(dst, frameTypeHardState)
	dst = bin.AppendUint64(dst, hs.Commit)
	dst = bin.AppendUint64(dst, hs.Term)
	dst = bin.AppendUint64(dst, hs.Vote)
	body := dst[bodyStart:]
	sum := crc32.ChecksumIEEE(body)
	dst = bin.AppendUint32(dst, sum)
	return dst
}

func appendIdxRecord(dst []byte, pt logPoint) []byte {
	dst = slices.Grow(dst, idxRecordSize)
	dst = bin.AppendUint64(dst, pt.idx)
	dst = bin.AppendUint64(dst, uint64(pt.off))
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

// readWALFrame returns either an entry or a HardState, plus the frame size
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
		return nil, nil, 0, bin.ErrCorruptState
	}

	bodyLen, _, err := bin.ReadUint32(hdr[:])
	if err != nil {
		return nil, nil, 0, err
	}
	switch {
	case bodyLen < frameEntryMetaLen:
		return nil, nil, 0, bin.ErrCorruptState
	case bodyLen > frameMaxBody:
		return nil, nil, 0, bin.ErrCorruptState
	}

	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, nil, 0, bin.ErrCorruptState
	}
	var crc [frameCRCLen]byte
	if _, err := io.ReadFull(r, crc[:]); err != nil {
		return nil, nil, 0, bin.ErrCorruptState
	}
	want, _, err := bin.ReadUint32(crc[:])
	if err != nil {
		return nil, nil, 0, err
	}
	if crc32.ChecksumIEEE(body) != want {
		return nil, nil, 0, bin.ErrCorruptState
	}

	frameSize := int64(frameHeaderSize + len(body) + frameCRCLen)

	kind, body, err := bin.ReadByte(body)
	if err != nil {
		return nil, nil, 0, err
	}

	if kind == frameTypeHardState {
		if bodyLen != frameHardStateLen {
			return nil, nil, 0, bin.ErrCorruptState
		}
		commit, body, err := bin.ReadUint64(body)
		if err != nil {
			return nil, nil, 0, err
		}
		term, body, err := bin.ReadUint64(body)
		if err != nil {
			return nil, nil, 0, err
		}
		vote, body, err := bin.ReadUint64(body)
		if err != nil {
			return nil, nil, 0, err
		}
		if len(body) != 0 {
			return nil, nil, 0, bin.ErrCorruptState
		}
		return nil, &raftpb.HardState{
			Commit: commit,
			Term:   term,
			Vote:   vote,
		}, frameSize, nil
	}

	index, body, err := bin.ReadUint64(body)
	if err != nil {
		return nil, nil, 0, err
	}
	term, body, err := bin.ReadUint64(body)
	if err != nil {
		return nil, nil, 0, err
	}
	return &raftpb.Entry{
		Type:  raftpb.EntryType(kind),
		Index: index,
		Term:  term,
		Data:  append([]byte(nil), body...),
	}, nil, frameSize, nil
}

func writeIdxPoint(f *os.File, pt logPoint) error {
	var buf []byte
	buf = bin.AppendUint64(buf, pt.idx)
	buf = bin.AppendUint64(buf, uint64(pt.off))
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
