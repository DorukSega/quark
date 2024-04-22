package main

import (
	"fmt"
	"io"
	"os"
	"time"
)
func readWithTime(file *os.File, db *DatabaseStructure, filename string, dst io.Writer) (difference time.Duration, err error) {
        start := time.Now()
        if err = read(file, db, filename, dst); err != nil {
            return 0, err
        }
        end := time.Now()
        difference = end.Sub(start)

		fmt.Println("[REPL] Duration for Read ", difference)
		timerWriter(filename, difference)
		readLog(db, filename)

        return difference, nil
}

func closeWithTime()  {
	message := "FILE CLOSED"
	timerWriter(message, 0)	
}

func reorgWithTime(file *os.File, db *DatabaseStructure)  {
	startMessage:= "- - - OPTIMIZER STARTED - - -"
	finishMessage := "- - - OPTIMIZER ENDED - - -"

	timerWriter(startMessage, 0)
	reorg(file, db, optimize_falgo(db))
	timerWriter(finishMessage, 0)
}

func print_help() {
	fmt.Println("\tread   <file> <order|optional>")
	fmt.Println("\twrite  <file> <order|optional>")
	fmt.Println("\toptimize1")
	fmt.Println("\tdelete <file>")
	fmt.Println("\tclose OR exit")
}