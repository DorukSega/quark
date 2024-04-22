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

	reader := csv.NewReader(file)
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
		for _, element := range lines {
			if strings.HasPrefix(element, "read") {
				args := strings.Split(element, " ")
				start := time.Now()
				var buffer bytes.Buffer
				read(myOS, db, args[1], &buffer)
				end := time.Now()
				duration := end.Sub(start)
				fmt.Println("[REPL] Duration for Read ", duration)
				timerWriter(args[1], duration)
				readLog(db, args[1])

			} else if strings.HasPrefix(element, "write") {
				args := strings.Split(element, " ")
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

				write(myOS, db, args[1], order)

			} else if strings.HasPrefix(element, "delete") {
				args := strings.Split(element, " ")
				if len(args) != 2 {
					fmt.Println("delete <filename>")
					continue ReadLoop
				}
				// Todo: When cache optimization is implemented, write only first to Stdout, cache rest
				delete(myOS, db, args[1])

			} else if strings.HasPrefix(element, "close") || strings.HasPrefix(element, "exit") {
				timerWriter("FILE CLOSED", 0)
				break ReadLoop

			} else if strings.HasPrefix(element, "optimize1") {
				timerWriter("- - - OPTIMIZER STARTED - - -", 0)
				reorg(file, db, optimize_falgo(db))
				timerWriter("- - - OPTIMIZER ENDED - - -", 0)

			} else if strings.HasPrefix(element, "help") {
				print_help()

			} else {
				fmt.Println("Unknown command.")
				print_help()

			}
		}
	}
}
