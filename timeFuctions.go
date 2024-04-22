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


