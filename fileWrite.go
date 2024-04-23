package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var skipper int = 3

func timerWriter(filename string, timer time.Duration) {
	binaryName := filepath.Base(os.Args[1])
	binaryName = strings.TrimSuffix(binaryName, ".bin")
	csvPath := "./logs/" + binaryName + "Timer" + ".csv"

	file_isnotexist := false

	_, err_stat := os.Stat(csvPath)
	file_isnotexist = os.IsNotExist(err_stat)

	// Open the CSV file in append mode
	file, err := os.OpenFile(csvPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("[READLOG] Error opening CSV file:", err)
		return
	}
	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	if file_isnotexist {
		headers := []string{"filename", "time"}
		if err := writer.Write(headers); err != nil {
			fmt.Println("Error writing headers to CSV:", err)
			return
		}
	}

	if filename == "- - - OPTIMIZER STARTED - - -" {
		newRow := []string{}
		writer.Write(newRow)

	} else if filename == "FILE CLOSED" {
		newRow := []string{}
		writer.Write(newRow)

	} else if filename == "- - - OPTIMIZER ENDED - - -" {
		newRow := []string{}
		writer.Write(newRow)

	} else if skipper == 3 {
		newRow := []string{}
		writer.Write(newRow)
		skipper = 0
	} else {
		skipper += 1
	}

	row := []string{filename, timer.String()}
	if err := writer.Write(row); err != nil {
		log.Fatal("[READLOG] Error writing row to CSV:", err)
		return
	}
	if filename == "- - - OPTIMIZER STARTED - - -" {
		newRow := []string{}
		writer.Write(newRow)
		skipper = -1

	} else if filename == "FILE CLOSED" {
		newRow := []string{}
		writer.Write(newRow)
		skipper = -1

	} else if filename == "- - - OPTIMIZER ENDED - - -" {
		newRow := []string{}
		writer.Write(newRow)

	}
}

func codeExecuter(myOS *os.File, db *DatabaseStructure, filepath string) {
	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	//open file for code execution and otomate for closing at the end

	reader := csv.NewReader(file)
	// reader for reading line by line

	for {
		record, err := reader.Read()
		var lines = make([]string, 0)
		lines = append(lines, strings.Join(record, ","))
		if err == io.EOF {
			break // End of file reached
		}
		if err != nil {
			log.Fatal(err)
		}
	ReadLoop:
		for _, command := range lines {			
			err	:=	userInputReceive(command, file, db)
			if err!= nil {
				break ReadLoop			
			}
		}
	}
}

func userInputReceive(command string, file *os.File, db *DatabaseStructure)(err error){	
	if strings.HasPrefix(command, "read") {
		/*	Todo: When cache optimization is implemented,
			write only first to Stdout, cache rest
		*/
		args := strings.Split(command, " ")
		// ["read", "test.txt"]
		if len(args) != 2 {
			fmt.Println("Please specify the file name like below:")
			fmt.Println("open <filename>")
			return nil
		}
		readWithTime(file, db, args[1], os.Stdout)
	} else if strings.HasPrefix(command, "memread") {
		args := strings.Split(command, " ")
		if len(args) != 2 {
			fmt.Println("Please specify the file name like below:")
			fmt.Println("open <filename>")
			return nil
		}
		var buffer bytes.Buffer
		fmt.Println("Before: ", buffer.Len())
		readWithTime(file, db, args[1], &buffer)
		fmt.Println("After: ", buffer.Len())
	} else if strings.HasPrefix(command, "write") {
		args := strings.Split(command, " ")
		// ["write", "test.txt"] or ["write", "test.txt", "3"]
		var order uint8 = db.RecordCount
		// place in database records

		if len(args) == 3 {
			// 3rd argument is order so conver into int
			t_ord, err := strconv.Atoi(args[2])
			if err != nil {
				fmt.Println("write <filename> <order|optional>")
				return nil
			}
			order = uint8(t_ord)
		} else if len(args) != 2 {
			fmt.Println("write <filename> <order|optional>")
			return nil
		}

		if err := write(file, db, args[1], order); err != nil {
			log.Fatal(err)
		}
	} else if strings.HasPrefix(command, "delete") {
		//	Todo: When cache optimization is implemented, write only first to Stdout, cache rest
		args := strings.Split(command, " ")
		if len(args) != 2 {
			fmt.Println("delete <filename>")
			return nil
		}
		delete(file, db, args[1])
	} else if strings.HasPrefix(command, "close") || strings.HasPrefix(command, "exit") {
		closeWithTime()
		return fmt.Errorf("close")
	} else if strings.HasPrefix(command, "optimize1") {
		reorgWithTime(file, db)
	} else if strings.HasPrefix(command, "code") {
		args := strings.Split(command, " ")
		if len(args) != 2 {
			fmt.Println("write <filename> <order|optional>")
			return nil
		}
		codeExecuter(file, db, args[1])
	} else if strings.HasPrefix(command, "help") {
		print_help()
	} else {
		fmt.Println("Unknown command. Please use one of the following: ")
		print_help()
	}
	return nil
}