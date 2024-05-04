package log_store

import (
	"fmt"
	"os"
	"path"

	api "commit_log/api/v1"

	"github.com/golang/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

type segmentReader struct {
	segment       *segment
	currentOffset uint64
}

func (segReader *segmentReader) Read(input []byte) (int, error) {
	bytes_read, err := segReader.segment.ReadRawRecord(input, segReader.currentOffset)
	if err != nil {
		return err
	}
	// Move the segmentReader memoized offset by one
	segReader.currentOffset++
	return bytes_read, err
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	storeFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	s.index, err = newIndex(indexFile, c)
	if err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append takes a record and return the offset of that record in the segment
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		uint32(s.nextOffset-s.baseOffset), pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

// Function takes in a relative offset and read the bytes into the input params
// in its bytes array form without marshalling the result
func (s *segment) ReadRawRecord(input []byte, off uint64) (int, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return 0, nil
	}
	// Potential issue here converting to int64 for input to store.ReadAt interface.
	bytes_read, err := s.store.ReadAt(input, int64(pos))
	if err != nil {
		return 0, nil
	}
	return bytes_read, err
}

func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// Returns the nearest and lesser multiple k and j
// Used to ensure we stay under user's disk capacity
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
