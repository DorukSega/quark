package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

/*
File Structure:
    Record Count - uint8
    Records:
        filename - [40]byte
        size 	 - int64
    Files:
        file 	 - any size
*/

// Record represents the structure of each record
type Record struct {
	FileName [40]byte // [40]byte
	Size     int64
}

// DatabaseStructure represents the overall structure of the database.
type DatabaseStructure struct {
	RecordCount uint8
	Records     []Record
}

var cursor_position int64 = 0

func main() {
	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatal("Usage: quark <database.db>")
	}
	filepath_db := flag.Arg(0)

	filepath_db = filepath.Clean(filepath_db)

	db_structure := DatabaseStructure{
		RecordCount: 0,
		Records:     []Record{},
	}

	if _, err := os.Stat(filepath_db); os.IsNotExist(err) {
		// file does not exist
		fmt.Println("[MAIN] Creating ", filepath_db)

		file, err := os.Create(filepath_db)
		if err != nil {
			log.Fatal("[MAIN] Error creating database: ", err)
		}

		// Write the first byte (0) to the file
		var first_byte uint8 = 0
		err = binary.Write(file, binary.LittleEndian, first_byte)
		if err != nil {
			log.Fatal("[MAIN] Error writing to database: ", err)
		}
		// move cursor after first byte
		cursor_position += binary_size(first_byte)

		// start the repl
		repl(file, &db_structure)
		//file.Close()

	} else if err != nil {
		log.Fatal(err)
	} else {
		// file exists
		fmt.Println("[MAIN] Reading ", filepath_db)
		// open file
		file, err := os.OpenFile(filepath_db, os.O_RDWR, os.ModePerm)
		if err != nil {
			log.Fatal("[MAIN] Error opening database: ", err)
		}

		//read first byte
		if err := binary.Read(file, binary.LittleEndian, &db_structure.RecordCount); err != nil {
			log.Fatal("[MAIN] Error reading first byte: ", err)
		}
		move_cursor(&db_structure.RecordCount)

		// Read Records
		for i := 0; i < int(db_structure.RecordCount); i++ {
			var record Record

			// Read filename
			if err := binary.Read(file, binary.LittleEndian, &record.FileName); err != nil {
				log.Fatal("[MAIN] Error reading FileName: ", err)
			}

			// Read size
			if err := binary.Read(file, binary.LittleEndian, &record.Size); err != nil {
				log.Fatal("[MAIN] Error reading size: ", err)
			}

			db_structure.Records = append(db_structure.Records, record)
		}
		move_cursor(&db_structure.Records)

		fmt.Printf("[MAIN] %s has %d records and cursor position is at %d\n", file.Name(), db_structure.RecordCount, cursor_position)
		repl(file, &db_structure)
		//file.Close()
	}

}

func repl(file *os.File, db *DatabaseStructure) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("[REPL] Starting Repl")
ReadLoop:
	for {
		fmt.Print("> ")
		scanner.Scan()
		command := scanner.Text()
		if strings.HasPrefix(command, "read") {
			args := strings.Split(command, " ")
			if len(args) != 2 {
				fmt.Println("open <filename>")
				continue ReadLoop
			}
			// Todo: When cache optimization is implemented, write only first to Stdout, cache rest
			read(file, db, args[1], os.Stdout)
		} else if strings.HasPrefix(command, "write") {
			args := strings.Split(command, " ")
			var order uint8 = db.RecordCount

			if len(args) == 3 {
				t_ord, err := strconv.Atoi(args[2])
				if err != nil {
					fmt.Println("write <filename> <order|optional>")
					continue ReadLoop
				}
				order = uint8(t_ord)
			} else if len(args) != 2 {
				fmt.Println("write <filename> <order|optional>")
				continue ReadLoop
			}

			write(file, db, args[1], order)

		} else if strings.HasPrefix(command, "close") {
			break ReadLoop
		} else {
			fmt.Println("Unknown command. Please enter read <file>, write <file>, close")
		}
	}

	file.Close()
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

// not concurrent for now
func write(file *os.File, db *DatabaseStructure, filepath string, order uint8) {
	if order > db.RecordCount {
		fmt.Println("[WRITE] Order is unusable")
		return
	}

	// open file
	new_file, err := os.Open(filepath)
	if err != nil {
		fmt.Println("[WRITE] Error opening source file: ", err)
		return
	}
	defer new_file.Close()

	// Get file size
	fileInfo, err := new_file.Stat()
	if err != nil {
		fmt.Println("[Write] Can't read file ", err)
		return
	}
	file_size := fileInfo.Size()
	file_name := fileInfo.Name()
	if record_contains(db, file_name) {
		fmt.Println("[Write] File already exists", file_name)
		return
	}
	// Create Record
	var record Record
	record.FileName = truncateString(file_name)
	record.Size = file_size

	fmt.Printf("[WRITE] Writing %s at %d with size %d\n", record.FileName, order, record.Size)

	// Create a temporary file for writing
	tempFile, err := os.CreateTemp("./", "tempfile")
	if err != nil {
		log.Fatal("[WRITE] Temporary file failed to create ", err)
	}

	metadata_point := binary_size(Record{}) * int64(order)

	// Write the first byte  to the file
	var first_byte uint8 = db.RecordCount + 1
	if err := binary.Write(tempFile, binary.LittleEndian, first_byte); err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[WRITE] Failed to write new record count ", err)
	}

	// Read data from the original file up to the record insertion point and write it to the temporary file
	_, err = file.Seek(binary_size(first_byte), io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[WRITE] Failed to seek start ", err)
	}

	_, err = io.CopyN(tempFile, file, metadata_point)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[WRITE] Failed to write the old metadata ", err)
	}

	//fmt.Println("[WRITE] metadata_point: ", metadata_point)

	// Write the new record
	if err := binary.Write(tempFile, binary.LittleEndian, record.FileName); err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[WRITE] Failed to write new record name ", err)
	}
	if err := binary.Write(tempFile, binary.LittleEndian, record.Size); err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[WRITE] Failed to write new record size ", err)
	}

	// get rest
	left_record_point := binary_size(Record{})*int64(db.RecordCount) - metadata_point

	_, err = io.CopyN(tempFile, file, left_record_point)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[WRITE] Failed to write the rest of metadata: ", err)
	}

	//fmt.Println("[WRITE] left_record_point: ", left_record_point)

	// insertion point
	var insertion_point int64 = 0
	for i := 0; i < int(order); i++ {
		insertion_point += db.Records[i].Size
	}

	//fmt.Println("[WRITE] insertion point: ", insertion_point)

	// Read data from the original file up to the file insertion point and write it to the temporary file
	_, err = io.CopyN(tempFile, file, insertion_point)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[WRITE] Failed to write the files before: ", err)
	}

	// Write new file
	_, err = io.Copy(tempFile, new_file)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[WRITE] Failed to write the new file ", err)
	}

	// Read the remaining data from the original file and write it to the temporary file
	_, err = io.Copy(tempFile, file)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[WRITE] Failed to write rest of the files ", err)
	}

	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[WRITE] Error going back to start in temp file ", err)
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[WRITE] Error going back to start in main file ", err)
	}

	_, err = io.Copy(file, tempFile)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[WRITE] Failed to write back to database ", err)
	}

	// get cursor pos
	n_seek, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[WRITE] Error getting cursor position ", err)
	}
	cursor_position = int64(n_seek)

	// Write new record in memory
	db.RecordCount += 1
	db.Records = append(db.Records, Record{})
	copy(db.Records[order+1:], db.Records[order:])
	db.Records[order] = record

	// Remove (delete) the temporary file
	tempFile.Close()
	err = os.Remove(tempFile.Name())
	if err != nil {
		log.Fatal("Error removing temporary file:", err)
	}

	fmt.Println("[WRITE] Write complete")
}

func read(file *os.File, db *DatabaseStructure, filename string, dst io.Writer) {
	// fail if we didn't write any files yet
	if db.RecordCount == 0 {
		fmt.Println("[READ] Database has no files")
		return
	}
	var file_size int64 = 0
	// calculate the location of file in the database
	var location int64 = binary_size(Record{})*int64(db.RecordCount) + binary_size(&db.RecordCount)
	for r_count, record := range db.Records {
		if record_name_compare(record.FileName, filename) {
			file_size = record.Size
			break
		}
		location += record.Size
		if r_count+1 == int(db.RecordCount) { // fail if you reached end
			// Todo: read fail case, should be something that programs can understand
			fmt.Println("[READ] No such file in database")
			return
		}
	}
	// seek to the location
	fmt.Println("[READ] Read location for debug purposes", location)

	new_offset, err := file.Seek(location, io.SeekStart)
	if err != nil {
		log.Fatal("[READ] Error seeking the location ", err)
	}

	// read and write to custom writer interface, quite often the stdout
	fmt.Println("[READ START]")

	_, err = io.CopyN(dst, file, file_size)
	if err != nil {
		log.Fatal("[READ] Failed reading file and writing to custom io: ", err)
	}
	cursor_position = new_offset + file_size

	fmt.Printf("\n[READ END]\n")
}
