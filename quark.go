package main

import (
	"bufio"
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

//MARK: File Structure
/*
File Structure:
    Record Count - uint8
    Records:
        filename - [40]byte
        size 	 - int64
    Files:
        file 	 - any size
----------------------------------------
test.bin =>
	total_record_count,
	records[file_name, file_size],
	record_data
*/

/*
MARK: Record
Record represents the structure of each record
*/
type Record struct {
	FileName [40]byte // [40]byte
	Size     int64
}

/*
MARK: DataBaseStucture
DatabaseStructure represents the overall structure of the database.
*/
type DatabaseStructure struct {
	RecordCount uint8
	Records     []Record
}

/*	MARK: ReadLog	*/
type Readlog struct {
	FileName string
	Time     int64
}

/*	MARK: Cursor Position	*/
var cursor_position int64 = 0

func main() {
	flag.Parse()

	//	Database first argument error check
	if flag.NArg() < 1 {
		log.Fatal("Usage: quark <database.db>")
	} else if name := flag.Arg(0); !strings.HasSuffix(name, ".bin") {
		text := fmt.Sprintf(`
		Error: The provided file '%s'
		Does not have the expected '.bin' extension.
		Please specify a binary file.
		`, name)
		log.Fatal(text)
	} else if name == ".bin" {
		text := fmt.Sprintf(`
		Error: The provided file name '%s' is invalid.
		Please specify a valid filename with 
		an appropriate name before the '.bin' extension.
		For example: 'test.bin'.
		`, name)
		log.Fatal(text)
	}

	filepath_db := flag.Arg(0)
	filepath_db = filepath.Clean(filepath_db)

	db_structure := DatabaseStructure{
		RecordCount: 0,
		Records:     []Record{},
	}
	// MARK: check file existence
	if _, err := os.Stat(filepath_db); os.IsNotExist(err) {
		// IF NOT EXIST
		fmt.Printf("[MAIN] Creating a binary file '%s'\n", filepath_db)
		file, err := os.Create(filepath_db)
		if err != nil {
			log.Fatal("[MAIN] Error creating database: ", err)
		}

		// MARK: First Byte
		var first_byte uint8 = 0
		err = binary.Write(file, binary.LittleEndian, first_byte)
		if err != nil {
			log.Fatal("[MAIN] Error writing to database: ", err)
		}
		// move cursor_position to first_byte
		cursor_position += binary_size(first_byte)

		// start the repl
		repl(file, &db_structure)
	} else if err != nil {
		log.Fatal(err)
	} else {
		//IF EXIST
		fmt.Printf("[MAIN] Reading the binary file'%s'\n", filepath_db)
		//File open with read-write permissions
		file, err := os.OpenFile(filepath_db, os.O_RDWR, os.ModePerm)
		if err != nil {
			log.Fatal("[MAIN] Error opening database: ", err)
		}
		/*	MARK:	READ record_count
			Read from the file
			But because &db_structure.RecordCount is uint8
			binary.Read() only reads uint8 size of file
			Value passed on become just first byte (record_count)
		*/
		if err := binary.Read(file, binary.LittleEndian, &db_structure.RecordCount); err != nil {
			log.Fatal("[MAIN] Error reading first byte: ", err)
		}
		move_cursor(&db_structure.RecordCount)
		//	Read each record
		for i := 0; i < int(db_structure.RecordCount); i++ {
			var record Record
			/* 	MARK:	READ filename
			Read filename size of [40 byte]
			*/
			if err := binary.Read(file, binary.LittleEndian, &record.FileName); err != nil {
				log.Fatal("[MAIN] Error reading FileName: ", err)
			}
			/* 	MARK:	READ filesize
			Read filename size of [8 byte]
			*/
			if err := binary.Read(file, binary.LittleEndian, &record.Size); err != nil {
				log.Fatal("[MAIN] Error reading size: ", err)
			}
			// read records in order and send them to main db
			db_structure.Records = append(db_structure.Records, record)
		}
		// move cursor up to total record counts
		move_cursor(&db_structure.Records)
		fmt.Printf("[MAIN] %s has %d records and cursor position is at %d\n", file.Name(), db_structure.RecordCount, cursor_position)
		fmt.Printf("\t%s\t-\t%s\n", "Record Name", "Record Size")
		for _, val := range db_structure.Records {
			fmt.Printf("\t%s\t\t-\t%v\n", val.FileName, val.Size)
		}

		repl(file, &db_structure)
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
		/*	MARK: User input
			Waits user input
		*/
		if strings.HasPrefix(command, "read") {
			/*	MARK: READ
				Todo: When cache optimization is implemented,
				write only first to Stdout, cache rest

				REDIS can be usefull

				AI: The suggested optimization involves implementing a
				caching mechanism. Instead of immediately writing all
				the file contents to os.Stdout, you would read and
				write only a portion of the file, while caching
				the remaining contents in memory or on disk. Subsequent
				reads would then fetch data from the cache rather
				than re-reading the entire file.
			*/
			args := strings.Split(command, " ")
			// ["read", "test.txt"]
			if len(args) != 2 {
				fmt.Println("Please specify the file name like below:")
				fmt.Println("open <filename>")
				continue ReadLoop
			}
			readWithTime(file, db, args[1], os.Stdout)
		} else if strings.HasPrefix(command, "write") {
			/*	MARKED: WRITE
			*/
			args := strings.Split(command, " ")
			// ["write", "test.txt"] or ["write", "test.txt", "3"]
			var order uint8 = db.RecordCount
			// place in database records

			if len(args) == 3 {
				// 3rd argument is order so conver into int
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

			if err := write(file, db, args[1], order); err != nil {
				log.Fatal(err)
			}			
		} else if strings.HasPrefix(command, "delete") {
			/*	MARKED: DELETE
					Todo: When cache optimization is implemented, 
					write only first to Stdout, cache rest
			*/
			args := strings.Split(command, " ")
			if len(args) != 2 {
				fmt.Println("delete <filename>")
				continue ReadLoop
			}
			delete(file, db, args[1])
		} else if strings.HasPrefix(command, "close") || strings.HasPrefix(command, "exit") {
			/*	MARKED: CLOSE
			*/
			closeWithTime()
			break ReadLoop
		} else if strings.HasPrefix(command, "optimize1") {
			/*	MARKED: OPTIMIZE
			*/
			reorgWithTime(file, db)
		} else if strings.HasPrefix(command, "code") {
			/*	MARKED: CODE
			*/
			args := strings.Split(command, " ")
			if len(args) != 2 {
				fmt.Println("write <filename> <order|optional>")
				continue ReadLoop
			}
			codeExecuter(file, db, args[1])
		} else if strings.HasPrefix(command, "help") {
			/*	MARKED: HELP
			*/
			print_help()
		} else {
			fmt.Println("Unknown command. Please use one of the following: ")
			print_help()
		}
	}
	defer file.Close()
}

func readLog(db *DatabaseStructure, filename string) {
	/* MARK: READLOG
			Writing read order of each read file
			filename	|	time
			1.txt		|	181.1µs
	*/
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
		// if file is not exist exit
		return
	}

	binaryName := filepath.Base(os.Args[1])
	// name of binary file "test.bin"
	csvPath := "./logs/" + binaryName + ".csv"
	// name of csv file "./logs/test.bin.csv"

	fileisnotexist := false
	_, err_stat := os.Stat(csvPath)
	fileisnotexist = os.IsNotExist(err_stat)
	//Check files existance

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

	if fileisnotexist {
		headers := []string{"filename", "time"}
		if err := writer.Write(headers); err != nil {
			log.Fatal("Error writing headers to CSV:", err)
			return
		}
	}

	fileReadTime := time.Now().Unix()

	row := []string{filename, strconv.FormatInt(fileReadTime, 10)}

	if err := writer.Write(row); err != nil {
		log.Fatal("[READLOG] Error writing row to CSV:", err)
		return
	}
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
		//changes
		timerWriter(byteReadable(v), 0)
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
	*falgo = append((*falgo)[:index], (*falgo)[index+1:]...)
}
