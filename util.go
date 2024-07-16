package main

import (
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

func logfilename(filename string) string {
	return fmt.Sprintf("%s.csv", filename)
}

func move_cursor(data any) {
	size := binary.Size(data)
	if size == -1 {
		return
	}
	cursor_position += int64(size)
}

func binary_size(data any) int64 {
	size := binary.Size(data)
	if size == -1 {
		return 0
	}
	return int64(size)
}

func truncateString(s string) [40]byte {
	// Convert the string to a byte slice
	stringBytes := []byte(s)

	// Create a fixed-size byte array of length 40
	var fixedSizeByteArray [40]byte

	// Copy the content of the byte slice into the fixed-size byte array
	copy(fixedSizeByteArray[:], stringBytes)

	return fixedSizeByteArray
}
func byteReadable(b [40]byte) string {
	return string(bytes.TrimRight(b[:], "\x00"))
}

func record_name_compare(record_filename [40]byte, target_filename string) bool {
	s_record_filename := string(bytes.TrimRight(record_filename[:], "\x00"))
	return s_record_filename == target_filename
}

func record_contains(db *DatabaseStructure, filename string) bool {
	for _, v := range db.Records {
		return record_name_compare(v.FileName, filename)
	}
	return false
}

func string_contains(slice []string, value string) bool {
	for _, item := range slice {
		if item == value {
			return true
		}
	}
	return false
}

func read_readlog(csvPath string) []Readlog {
	file, err := os.OpenFile(csvPath, os.O_RDONLY, 0644)
	if err != nil {
		fmt.Println("[READLG] No log file:", err)
		return nil
	}
	defer file.Close()
	reader := csv.NewReader(file)

	_, err = reader.Read()
	if err != nil {
		fmt.Println("[READLG] Reader can't read:", err)
		return nil
	}

	records := []Readlog{}
	for {
		raw_record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		time, err2 := strconv.ParseInt(raw_record[1], 10, 64)
		if err2 != nil {
			log.Fatal(err)
		}
		records = append(records, Readlog{
			FileName: raw_record[0],
			Time:     time,
		})
	}
	return records
}

func create_file(filepath_db string) *os.File {
	file, err := os.Create(filepath_db)
	if err != nil {
		log.Fatal("[MAIN] Error creating database: ", err)
	}

	// First Byte
	var first_byte uint8 = 0
	err = binary.Write(file, binary.LittleEndian, first_byte)
	if err != nil {
		log.Fatal("[MAIN] Error writing to database: ", err)
	}
	// move cursor_position to first_byte
	cursor_position += binary_size(first_byte)
	return file
}

type QueueRecord struct {
	FileName string
	SizeRead int64
}

type Queue[T any] interface {
	Enqueue(data T)
	Dequeue() (T, bool)
	Peek() (T, bool)
	IsEmpty() bool
}

// SliceQueue implements the Queue interface using a slice
type SliceQueue[T any] struct {
	items []T
}

// NewSliceQueue creates a new SliceQueue
func NewSliceQueue[T any]() *SliceQueue[T] {
	return &SliceQueue[T]{}
}

// Enqueue adds an element to the back of the queue
func (q *SliceQueue[T]) Enqueue(data T) {
	q.items = append(q.items, data)
}

// Dequeue removes and returns the element at the front of the queue
// Returns false if the queue is empty
func (q *SliceQueue[T]) Dequeue() (T, bool) {
	if q.IsEmpty() {
		return *new(T), false
	}
	first := q.items[0]
	q.items = q.items[1:]
	return first, true
}

// Peek returns the element at the front of the queue without removing it
// Returns false if the queue is empty
func (q *SliceQueue[T]) Peek() (T, bool) {
	if q.IsEmpty() {
		return *new(T), false
	}
	return q.items[0], true
}

// IsEmpty checks if the queue is empty
func (q *SliceQueue[T]) IsEmpty() bool {
	return len(q.items) == 0
}
