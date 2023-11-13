package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

/*
File Structure:
    Record Count - uint8
    Records:
        filename - [40]byte
        order - uint8
        size - uint64
    Files:
        file - any size
*/

// Record represents the structure of each record in the file.
type Record struct {
	FileName string
	Order    uint8
	Size     uint64
}

// FileStructure represents the overall structure of the file.
type FileStructure struct {
	RecordCount uint8
	Records     []Record
	Files       [][]byte
}

var cursor_position uint64 = 0

func main() {
	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatal("Usage: quark <database.db>")
	}
	filepath_db := flag.Arg(0)

	filepath_db = filepath.Clean(filepath_db)

	file_structure := FileStructure{
		RecordCount: 0,
		Records:     []Record{},
		Files:       [][]byte{},
	}

	if _, err := os.Stat(filepath_db); os.IsNotExist(err) {
		// file does not exist
		fmt.Println("[MAIN] Creating ", filepath_db)

		file, err := os.Create(filepath_db)
		if err != nil {
			log.Fatal("[MAIN] Error creating file: ", err)
		}

		// Write the first byte (0) to the file
		write_n, err := file.Write([]byte{0})
		if err != nil {
			log.Fatal("[MAIN] Error writing to file: ", err)
		}
		// move cursor after first byte
		cursor_position += uint64(write_n)

		// start the repl
		repl(file, &file_structure)
		//file.Close()

	} else if err != nil {
		log.Fatal(err)
	} else {
		// file exists
		fmt.Println("[MAIN] Reading ", filepath_db)
		// open file
		file, err := os.Open(filepath_db)
		if err != nil {
			log.Fatal("[MAIN] Error Opening File: ", err)
		}

		//read first byte
		if err := binary.Read(file, binary.LittleEndian, &file_structure.RecordCount); err != nil {
			log.Fatal("[MAIN] Error Reading First Byte: ", err)
		}
		move_cursor(&file_structure.RecordCount)

		// Read Records
		for i := 0; i < int(file_structure.RecordCount); i++ {
			var record Record

			// Read filename
			filenameBytes := make([]byte, 40)
			if _, err := file.Read(filenameBytes); err != nil {
				log.Fatal("[MAIN] Error Reading Filename: ", err)
			}
			record.FileName = string(filenameBytes)

			// Read order
			if err := binary.Read(file, binary.LittleEndian, &record.Order); err != nil {
				log.Fatal("[MAIN] Error Reading Order: ", err)
			}

			// Read size
			if err := binary.Read(file, binary.LittleEndian, &record.Size); err != nil {
				log.Fatal("[MAIN] Error Reading Size: ", err)
			}

			file_structure.Records = append(file_structure.Records, record)
		}
		move_cursor(&file_structure.Records)

		fmt.Printf("[MAIN] %s has %d records and cursor position is at %d\n", file.Name(), file_structure.RecordCount, cursor_position)
		repl(file, &file_structure)
		//file.Close()
	}

}

func repl(file *os.File, fs *FileStructure) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("[REPL] Starting Repl")
ReadLoop:
	for {
		fmt.Print("> ")
		scanner.Scan()
		command := scanner.Text()

		switch {
		case command == "read":
			//readFile()
			fmt.Println("todo read ")
		case command == "write":
			//writeFile()
			fmt.Println("todo write")
		case command == "close":
			break ReadLoop
		default:
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
	cursor_position += uint64(size)
}
