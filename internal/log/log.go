package log_store

import (
	api "commit_log/api/v1"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Log holds many segments
type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	// Current active segment to append to
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	log := &Log{
		Dir:    dir,
		Config: c,
	}
	return log, log.setup()
}

func (log *Log) newSegment(baseOffset uint64) error {
	s, err := newSegment(log.Dir, baseOffset, log.Config)
	if err != nil {
		return err
	}
	log.segments = append(log.segments, s)
	log.activeSegment = s
	return nil
}

func (log *Log) setup() error {
	files, err := os.ReadDir(log.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		// We get the base offset from the string we create the segment with
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	// We sort the base offsets from oldest to newest
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err = log.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffsets contain duplicates for .index and .store, so we skip the duplicate
		i++
	}
	// If the log has no segments, we create the inital one.
	if log.segments == nil {
		if err = log.newSegment(
			log.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}
	}
	return nil
}

// Takes in a record and return the offset to the record that was appended.
func (log *Log) Append(record *api.Record) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	off, err := log.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	// Note, we grab a mutex on the log to hold this section of code. You could optimize this to coordinate access of locks per segment rather than the whole log
	if log.activeSegment.IsMaxed() {
		err = log.newSegment(off + 1)
	}
	return off, err
}

// Takes in an offset and return the record at that offset.
func (log *Log) Read(off uint64) (*api.Record, error) {
	// Loop through segments and find the first segment that might contain our offset to read from
	log.mu.RLock()
	defer log.mu.RUnlock()

	var s *segment
	for _, segment := range log.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	return s.Read(off)
}

func (log *Log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()

	for _, segment := range log.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (log *Log) Remove() error {
	if err := log.Close(); err != nil {
		return err
	}
	return os.RemoveAll(log.Dir)
}

func (log *Log) Reset() error {
	if err := log.Remove(); err != nil {
		return err
	}
	return log.setup()
}

func (log *Log) LowestOffset() (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	return log.segments[0].baseOffset, nil
}

func (log *Log) HighestOffset() (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()

	offset := log.segments[len(log.segments)-1].nextOffset - 1
	if offset == 0 {
		return 0, nil
	}
	return offset, nil
}

// Truncate is used to accept an offset and remove all segments lower than the given lowest offset
func (log *Log) Truncate(lowest uint64) error {
	log.mu.Lock()
	defer log.mu.Unlock()

	var newSegments []*segment
	for _, segment := range log.segments {
		if segment.nextOffset-1 <= lowest {
			if err := segment.Remove(); err != nil {
				return err
			}
			continue
		}
		newSegments = append(newSegments, segment)
	}
	log.segments = newSegments
	return nil
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}
