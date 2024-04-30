package log_store

import (
	api "commit_log/api/v1"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment-test")
	defer os.RemoveAll(dir)

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3
	s, err := newSegment(dir, 0, c)
	require.NoError(t, err)
	sample_record := &api.Record{Value: []byte("hello world")}

	for i := uint64(0); i < 3; i++ {
		testSegmentAppend(t, s, sample_record)
		testSegmentRead(t, s, i, sample_record)
	}
	fourth_record := &api.Record{Value: []byte("fourth record")}
	_, err = s.Append(fourth_record)
	require.Equal(t, io.EOF, err)

	require.Equal(t, true, s.IsMaxed())

	c.Segment.MaxStoreBytes = uint64(len(sample_record.Value))
	c.Segment.MaxIndexBytes = 1024

	// Assert that the index has plenty of space
	require.True(t, c.Segment.MaxIndexBytes > entWidth*3)

	s, err = newSegment(dir, 0, c)
	require.NoError(t, err)

	require.True(t, true, s.IsMaxed())
	err = s.Remove()
	require.NoError(t, err)

	s, err = newSegment(dir, 0, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}

func testSegmentAppend(t *testing.T, s *segment, r *api.Record) {
	currentNextOffset := s.nextOffset
	offset, err := s.Append(r)
	require.NoError(t, err)

	require.Equal(t, currentNextOffset, offset)

}

func testSegmentRead(t *testing.T, s *segment, offset uint64, expectedRecord *api.Record) {
	record, err := s.Read(offset)
	require.NoError(t, err)
	require.Equal(t, record, record)
}
