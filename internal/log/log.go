package log_store

import (
	api "commit_log/api/v1"
	"io/ioutil"
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

func setup(log *Log) error {
	files, err := ioutil.ReadDir(log.Dir)
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
