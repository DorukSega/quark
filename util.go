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
		log.Fatal("[READLG] No log file:", err)
		return nil
	}
	defer file.Close()
	reader := csv.NewReader(file)

	_, err = reader.Read()
	if err != nil {
		log.Fatal("[READLG] Reader can't read:", err)
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
