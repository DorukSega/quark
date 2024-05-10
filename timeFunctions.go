package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"time"
)

func readWithTime(file *os.File, db *DatabaseStructure, filename string, dst io.Writer) {
	var difference time.Duration
	start := time.Now()
	if err := read(file, db, filename, dst); err != nil {
		fmt.Println(err)
		return
	}
	end := time.Now()
	difference = end.Sub(start)

	timerWriter(filename, difference)
	readLog(os.Args[1], db, filename)
}

func closeWithTime() {
	message := "FILE CLOSED"
	timerWriter(message, 0)
}

func reorgWithTime(file *os.File, db *DatabaseStructure) {
	optimize_falgo(file, db, os.Args[1])
}

func print_help() {
	fmt.Println("\tread  	 <file> <order|optional>")
	fmt.Println("\twrite  	 <file> <order|optional>")
	fmt.Println("\tmemread   <file> <order|optional>")
	fmt.Println("\tdelete 	 <file>")
	fmt.Println("\toptimize1")
	fmt.Println("\tclose OR exit")
}

func timed_execute(filepath string, n int) {
	// recreate database
	// clear readlog
	// read file in filepath
	// create file up to write
	// write them
	// read file to a slice
	// get OPTIMIZE FLAG
	// n times:
	// 		start timer
	// 		read files from slice
	// 		end timer
	// get the average time
	// OPTIMIZE_ALGO()
	// n times:
	// 		start timer
	// 		read files from slice
	// 		end timer
	// get the average time
	// print results
	db_name := "opt_test.bin"
	os.Remove(db_name)

	csvPath := "./logs/" + logfilename(db_name)
	os.Remove(csvPath)

	file := create_file(db_name)
	db := DatabaseStructure{
		RecordCount: 0,
		Records:     []Record{},
	}
	defer file.Close()
	code_file, err2 := os.OpenFile(filepath, os.O_RDONLY, 0644)
	if err2 != nil {
		fmt.Printf("[TIMED] No code file: %s\n", err2)
		return
	}
	defer code_file.Close()

	var to_write = make([]string, 0)
	var to_read = make([]string, 0)

	scanner := bufio.NewScanner(code_file)
	var flag = 0
	var opt_func func(file *os.File, db *DatabaseStructure, binaryName string)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if line == "WRITE" {
			flag = 1
			continue
		} else if line == "OPTIMIZE1" {
			opt_func = optimize_falgo
			break
		}

		if flag == 0 {
			to_write = append(to_write, line)
		} else if flag == 1 {
			to_read = append(to_read, line)
		}
	}
	if len(to_read) < 1 || len(to_write) < 1 {
		fmt.Println("[TIMED] code file is invalid")
		return
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("[TIMED] Error scanning file: %s\n", err)
		return
	}

	for _, fpath := range to_write {
		fileSize := 100 * 1024 * 1024 // 100mb

		f, err := os.Create(fpath)
		if err != nil {
			panic(err)
		}
		// Write the random data to the file
		written := 0
		for written < fileSize {
			fbuffer := make([]byte, 4096) // Buffer size can be adjusted
			n, err := rand.Read(fbuffer)
			if err != nil {
				fmt.Println("Error reading random data:", err)
				return
			}
			written += n
			_, err = f.Write(fbuffer[:n]) // Write only the actual number of bytes read
			if err != nil {
				fmt.Println("Error writing to file:", err)
				return
			}
		}
		f.Close()
	}

	for _, fpath := range to_write {
		write(file, &db, fpath, db.RecordCount)
	}

	debug.FreeOSMemory()

	var dur_unopt time.Duration

	var buffer *bytes.Buffer = bytes.NewBuffer([]byte{1})

	var start_unopt time.Time
	var end_unopt time.Time
	for i := 0; i < n; i++ {
		for _, fname := range to_read {
			start_unopt = time.Now()
			if err := read(file, &db, fname, buffer); err != nil {
				fmt.Println(err)
				return
			}
			end_unopt = time.Now()
			buffer.Reset()
			buffer = bytes.NewBuffer([]byte{1})
			debug.FreeOSMemory()
			if i == 0 {
				readLog(db_name, &db, fname)
			}
			dur_unopt += end_unopt.Sub(start_unopt)
		}
	}

	buffer = bytes.NewBuffer([]byte{2})
	buffer.Reset()

	debug.FreeOSMemory()
	fmt.Println("OPT START")
	opt_func(file, &db, db_name)
	fmt.Println("OPT END")
	debug.FreeOSMemory()

	var dur_opt time.Duration

	var start_opt time.Time
	var end_opt time.Time
	for i := 0; i < n; i++ {
		for _, fname := range to_read {
			start_opt = time.Now()
			if err := read(file, &db, fname, buffer); err != nil {
				fmt.Println(err)
				return
			}
			end_opt = time.Now()
			buffer.Reset()
			buffer = bytes.NewBuffer([]byte{2})
			debug.FreeOSMemory()
			dur_opt += end_opt.Sub(start_opt)
		}
	}

	buffer.Reset()

	fmt.Printf("[TIME] Before Optimization: %v\n", dur_unopt)
	fmt.Printf("[TIME] After Optimization: %v\n", dur_opt)
	fmt.Printf("[TIME]  %d%% Faster\n", (((dur_unopt - dur_opt) * 100) / dur_unopt))

	err := os.Remove(db_name)
	if err != nil {
		fmt.Printf("[TIMED] can't remove file: %v\n", err)
		return
	}

	err = os.Remove(csvPath)
	if err != nil {
		fmt.Printf("[TIMED] can't remove file: %v\n", err)
		return
	}

	debug.FreeOSMemory()
}
