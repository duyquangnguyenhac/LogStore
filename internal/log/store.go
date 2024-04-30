package log_store

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	length_of_uint64_in_bytes = 8
)

// store is a file where we store our records
type store struct {
	file *os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi_info, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi_info.Size())
	return &store{
		file: f,
		buf:  bufio.NewWriter(f),
		size: size,
	}, nil
}

// Append takes in a bytes array to write in our record.
// Returns number of bytes written, the position, and error
func (s *store) Append(arr []byte) (uint64, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos := s.size

	// Write the size of the Record at the start of the record.
	err := binary.Write(s.buf, binary.BigEndian, uint64(len(arr)))
	if err != nil {
		return 0, 0, err
	}
	// Write the actual bytes data of the record
	num_bytes_written, err := s.buf.Write(arr)
	if err != nil {
		return 0, 0, err
	}
	num_bytes_written += length_of_uint64_in_bytes
	// Increment the size of the Store
	s.size += uint64(num_bytes_written)
	// Return the position before the record size is increment at the start of our record
	return uint64(num_bytes_written), pos, err
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// First we flush the buffer.
	err := s.buf.Flush()
	if err != nil {
		return nil, err
	}
	size := make([]byte, length_of_uint64_in_bytes)
	_, err = s.file.ReadAt(size, int64(pos))
	if err != nil {
		return nil, err
	}
	// Convert the binary array into uint64 and create another bytes array of that size to hold the record's data
	record_bytes_arr := make([]byte, binary.BigEndian.Uint64(size))
	_, err = s.file.ReadAt(record_bytes_arr, int64(pos+length_of_uint64_in_bytes))
	if err != nil {
		return nil, err
	}
	return record_bytes_arr, nil
}

// Reimplement the io.ReadAt interface
// ReadAt takes in a byte array and an offset
// It then read from the logStore at the given offset into the bytes array
func (s *store) ReadAt(p []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return 0, err
	}
	return s.file.ReadAt(p, offset)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	err = s.file.Close()
	if err != nil {
		return err
	} else {
		return nil
	}
}

func (s *store) Name() string {
	return s.file.Name()
}
