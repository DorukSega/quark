package main

import (
	"bytes"
	"encoding/binary"
)

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

func falgo_contains(falgo *[]Falgo, filename string) bool {
	for _, v := range *falgo {
		return v.FileName == filename
	}
	return false
}

func edges_contains(edges *[]Edge, to_filename string) bool {
	for _, v := range *edges {
		return v.ToFilename == to_filename
	}
	return false
}
