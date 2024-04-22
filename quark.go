package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
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

type Readlog struct {
	FileName string
	Time     int64
}

var cursor_position int64 = 0

func main() {
	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatal("Usage: quark <database.db>")
	}
	//code start pont
	filepath_db := flag.Arg(0)

	filepath_db = filepath.Clean(filepath_db)
	//command line clearance

	db_structure := DatabaseStructure{
		RecordCount: 0,
		Records:     []Record{},
	}
	/*
		main data structure
		recordCounter number of files in database
		Records filename 40 byte + size of the file
	*/

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
		for _, val := range db_structure.Records {
			fmt.Printf("\t%s - %v\n", val.FileName, val.Size)
		}

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
			start := time.Now()
			read(file, db, args[1], os.Stdout)
			end := time.Now()
			duration := end.Sub(start)
			fmt.Println("[REPL] Duration for Read ", duration)
			readLog(db, args[1])
			// Todo(salih): readlog(file.Name(), args[1])
		} else if strings.HasPrefix(command, "memread") {
			args := strings.Split(command, " ")
			if len(args) != 2 {
				fmt.Println("open <filename>")
				continue ReadLoop
			}

			start := time.Now()
			var buffer bytes.Buffer
			fmt.Println("Before: ", buffer.Len())
			read(file, db, args[1], &buffer)
			fmt.Println("After: ", buffer.Len())
			end := time.Now()

			duration := end.Sub(start)
			fmt.Println("[REPL] Duration for Read ", duration)
			readLog(db, args[1])
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

		} else if strings.HasPrefix(command, "delete") {
			args := strings.Split(command, " ")
			if len(args) != 2 {
				fmt.Println("delete <filename>")
				continue ReadLoop
			}
			// Todo: When cache optimization is implemented, write only first to Stdout, cache rest
			delete(file, db, args[1])

		} else if strings.HasPrefix(command, "close") || strings.HasPrefix(command, "exit") {
			break ReadLoop
		} else if strings.HasPrefix(command, "optimize1") {
			reorg(file, db, optimize_falgo(db))
		} else if strings.HasPrefix(command, "help") {
			print_help()
		} else {
			fmt.Println("Unknown command.")
			print_help()
		}
	}

	file.Close()
}

func print_help() {
	fmt.Println("\tread    	 <file> <order|optional>")
	fmt.Println("\twrite  	 <file> <order|optional>")
	fmt.Println("\tdelete 	 <file>")
	fmt.Println("\rmemread   <file> <order|optional>")
	fmt.Println("\tclose OR exit")
}

func readLog(db *DatabaseStructure, filename string) {
	// fail if we didn't write any files yet
	if db.RecordCount == 0 {
		return
	}
	fileCheck := false
	for _, record := range db.Records {
		if record_name_compare(record.FileName, filename) {
			fileCheck = true
			break
		}
	}
	if !fileCheck {
		return
	}

	binaryName := filepath.Base(os.Args[1])
	csvPath := "./logs/" + binaryName + ".csv"

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

	fileReadTime := time.Now().Unix()

	row := []string{filename, strconv.FormatInt(fileReadTime, 10)}

	if err := writer.Write(row); err != nil {
		log.Fatal("[READLOG] Error writing row to CSV:", err)
		return
	}

	//fmt.Println("Binary file name:", binaryName)
	//fmt.Println("CSV file name:", csvName)
	//fmt.Println("filename file name:", filename)
}

/*
FALGO_RECORDS:

	Falgo:
		filename
		total_weight
		Edges[]:
			to_filename
			weight
*/
type Edge struct {
	ToFilename string
	Weight     int
}

type Falgo struct {
	FileName    string
	TotalWeight int
	Edges       []Edge
}

func optimize_falgo(db *DatabaseStructure) [][40]byte {
	binaryName := filepath.Base(os.Args[1])
	csvPath := "./logs/" + binaryName + ".csv"

	file, err := os.OpenFile(csvPath, os.O_RDONLY, 0644)
	if err != nil {
		log.Fatal("[OPTMZ1] Error opening CSV file:", err)
		return nil
	}
	defer file.Close()
	reader := csv.NewReader(file)

	_, err = reader.Read()
	if err != nil {
		log.Fatal("[OPTMZ1] Reader can't read:", err)
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
	//fmt.Println(records)
	falgo_records := []Falgo{}
	for _, recdb := range db.Records {
		falgo_records = append(falgo_records, Falgo{
			FileName:    byteReadable(recdb.FileName),
			TotalWeight: 0,
			Edges:       []Edge{},
		})
	}

	for ir, rec := range records {
		if ir+1 == len(records) {
			break
		}
		for ix := range falgo_records {
			fal := &falgo_records[ix]
			if fal.FileName == rec.FileName {
				rec_next := records[ir+1]
				if fal.FileName == rec_next.FileName {
					fal.TotalWeight++
					break // break from upper for
				}
				// check if there is a edge, if so use
				// else add a new
				if edges_contains(&fal.Edges, rec_next.FileName) {
					for iy := range fal.Edges {
						edge := &fal.Edges[iy]
						if edge.ToFilename == rec_next.FileName {
							edge.Weight++
							break
						}
					}
				} else {
					fal.Edges = append(fal.Edges, Edge{
						ToFilename: rec_next.FileName,
						Weight:     1,
					})
				}
				fal.TotalWeight++
				break
			}
		}
	}
	//fmt.Println(falgo_records)
	n_db := [][40]byte{}
	// start with largest total weight, go to largest edge
	// continue until consumed, start again with the rest
	for len(falgo_records) != 0 {
		sort.Slice(falgo_records, func(i, j int) bool {
			return falgo_records[i].TotalWeight > falgo_records[j].TotalWeight
		})
		falgo_recursive(&n_db, &falgo_records, 0)
	}
	for _, v := range n_db {
		fmt.Println(byteReadable(v))
	}
	return n_db
}

func falgo_recursive(n_db *[][40]byte, falgo *[]Falgo, index int) {
	val := (*falgo)[index]
	sort.Slice(val.Edges, func(i, j int) bool {
		return val.Edges[i].Weight > val.Edges[j].Weight
	})
	var selected_edge string = ""
	for _, esel := range val.Edges {
		if selected_edge != "" {
			break
		}
		for _, v := range *falgo {
			if v.FileName == esel.ToFilename {
				selected_edge = esel.ToFilename
				break
			}
		}
	}

	// write
	*n_db = append(*n_db, truncateString(val.FileName))

	if selected_edge == "" {
		*falgo = append((*falgo)[:index], (*falgo)[index+1:]...)
		return
	}
	next_index := 0
	for iy, nval := range *falgo {
		if nval.FileName == selected_edge {
			next_index = iy
			break
		}
	}
	if next_index != 0 {
		falgo_recursive(n_db, falgo, next_index)
	}
	if index < 0 || index >= len(*falgo) {
		fmt.Println("Error: Index is out of bounds")
		return
	}
	*falgo = append((*falgo)[:index], (*falgo)[index+1:]...)
}
