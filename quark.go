package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"flag"
	"fmt"
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

		if strings.HasPrefix(command, "read") {
			/*	MARK: READ
				Todo: When cache optimization is implemented,
				write only first to Stdout, cache rest
			*/
			args := strings.Split(command, " ")
			// ["read", "test.txt"]
			if len(args) != 2 {
				fmt.Println("Please specify the file name like below:")
				fmt.Println("open <filename>")
				continue ReadLoop
			}
			readWithTime(file, db, args[1], os.Stdout)
		} else if strings.HasPrefix(command, "memread") {
			args := strings.Split(command, " ")
			if len(args) != 2 {
				fmt.Println("Please specify the file name like below:")
				fmt.Println("open <filename>")
				continue ReadLoop
			}
			var buffer bytes.Buffer
			fmt.Println("Before: ", buffer.Len())
			readWithTime(file, db, args[1], &buffer)
			fmt.Println("After: ", buffer.Len())
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
	file.Close()
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
	// name of binary file "filename"
	csvPath := "./logs/" + logfilename(binaryName)
	// name of csv file "./logs/filename.csv"

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

type FileMap map[string]EFileInfo // from filename to maximum edge

type EFilePair struct {
	Fname string
	Info  EFileInfo
}

type EFileInfo struct {
	TotalWeight int
	MaxEdges    []string
}

func optimize_falgo(file *os.File, db *DatabaseStructure) {
	binaryName := filepath.Base(os.Args[1])
	csvPath := "./logs/" + binaryName + ".csv"
	records := read_readlog(csvPath)
	if records == nil {
		return // nothing to optimize
	}
	falgo_pslice := make([]EFilePair, 0)
	// init all edges
	for _, recdb := range db.Records {
		fnname := byteReadable(recdb.FileName)
		falgo_pslice = append(falgo_pslice, EFilePair{
			Fname: fnname,
			Info:  calculate_occurance(records, fnname),
		})
	}
	// sort them
	sort.Slice(falgo_pslice, func(i, j int) bool {
		return falgo_pslice[i].Info.TotalWeight > falgo_pslice[j].Info.TotalWeight
	})
	final_res := make([]string, 0)
	if len(falgo_pslice) < 1 {
		// TODO: add error log
		return
	}
	falgo := falgo_pslice[0]
	final_res = append(final_res, falgo.Fname)
	var next_falgo = ""
	if len(falgo.Info.MaxEdges) > 0 {
		next_falgo = falgo.Info.MaxEdges[0]
		final_res = append(final_res, next_falgo)
	}
	var new_pairs []string = nil
MainLoop:
	for {
		new_pairs = find_occurance(falgo_pslice, next_falgo)
		if new_pairs == nil {
			for _, val := range falgo_pslice {
				if !string_contains(final_res, val.Fname) {
					new_pairs = find_occurance(falgo_pslice, val.Fname)
					final_res = append(final_res, val.Fname)
					break
				}
			}
			if new_pairs == nil {
				break MainLoop
			}
		}
		for _, nexter_fname := range new_pairs {
			if nexter_fname == "" {
				next_falgo = ""
				break
			}
			if !string_contains(final_res, nexter_fname) {
				final_res = append(final_res, nexter_fname)
				next_falgo = nexter_fname
				break
			}
		}
	}

	n_db := [][40]byte{}
	fmt.Println(final_res)
	for _, value := range final_res {
		n_db = append(n_db, truncateString(value))
	}
	reorg(file, db, n_db)
}
func find_occurance(falgo_pslice []EFilePair, next_falgo string) []string {
	if next_falgo == "" {
		return nil
	}
	for _, val := range falgo_pslice {
		if val.Fname != next_falgo {
			continue
		}
		return val.Info.MaxEdges
	}
	return nil
}

func calculate_occurance(records []Readlog, fnname string) EFileInfo {
	var total_weight = 0

	var weight_map = make(map[string]int)
	for ir, rec := range records {
		cur_fname := rec.FileName
		if cur_fname != fnname {
			continue
		}
		if ir+1 == len(records) {
			total_weight++
			break
		}
		next_fname := records[ir+1].FileName
		if cur_fname == next_fname {
			total_weight++
			continue
		}
		total_weight++
		weight_map[next_fname] += 1
	}

	type Pair struct {
		Key   string
		Value int
	}
	var pairs []Pair
	for k, v := range weight_map {
		pairs = append(pairs, Pair{k, v})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Value < pairs[j].Value
	})

	max_edges := make([]string, 3)
	for ix, v := range pairs {
		if ix > 2 {
			break
		}
		max_edges[ix] = v.Key
	}
	return EFileInfo{
		TotalWeight: total_weight,
		MaxEdges:    max_edges,
	}
}
