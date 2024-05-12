package main

import (
	"fmt"
	"io"
	"os"
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
	optimize_algo1(file, db, get_occurance_slice(db, os.Args[1]))
}
