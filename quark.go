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
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
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
----------------------------------------
test.bin =>
	total_record_count,
	records[file_name, file_size],
	record_data
*/

type Record struct {
	FileName [40]byte // [40]byte
	Size     int64
}

type DatabaseStructure struct {
	RecordCount uint8
	Records     []Record
}

type Readlog struct {
	FileName string
	Time     int64
}

type FileMap map[string]EFileInfo // from filename to maximum edge

type EFilePair struct {
	Fname string
	Info  EFileInfo
}

type EFileInfo struct {
	TotalWeight int
	MaxEdges    []string
}

// GLOBAL TYPES
var cursor_position int64 = 0
var last_fileinfo []EFilePair
var opt2_flag bool = false
var file_buffer_map = make(map[string]*bytes.Buffer)
var IdleQueue = NewSliceQueue[QueueRecord]()

var DATABASE_LOCK sync.Mutex = sync.Mutex{}

func main() {
	flag.Parse()
	//	Database first argument error check
	if flag.NArg() < 1 {
		log.Fatal("Usage: quark <database.db>")
	}

	filepath_db := flag.Arg(0)
	if filepath_db == "time" {
		opt_file := flag.Arg(1)
		opt_file = filepath.Clean(opt_file)
		n := 1
		var err error // atoi error
		if flag.NArg() == 3 {
			n, err = strconv.Atoi(flag.Arg(2))
			if err != nil {
				log.Fatal("Usage: quark time <file> <times>", err)
			}
		}
		timed_execute(opt_file, n)
		return
	}
	filepath_db = filepath.Clean(filepath_db)

	db_structure := DatabaseStructure{
		RecordCount: 0,
		Records:     []Record{},
	}
	if _, err := os.Stat(filepath_db); os.IsNotExist(err) {
		// IF NOT EXIST
		fmt.Printf("[MAIN] Creating a database file '%s'\n", filepath_db)
		file := create_file(filepath_db)
		// start the repl
		repl(file, &db_structure)
	} else if err != nil {
		log.Fatal(err)
	} else {
		//IF EXIST
		fmt.Printf("[MAIN] Reading the database file %q\n", filepath_db)
		//File open with read-write permissions
		file, err := os.OpenFile(filepath_db, os.O_RDWR, os.ModePerm)
		if err != nil {
			log.Fatal("[MAIN] Error opening database: ", err)
		}
		if err := binary.Read(file, binary.LittleEndian, &db_structure.RecordCount); err != nil {
			log.Fatal("[MAIN] Error reading first byte: ", err)
		}
		move_cursor(&db_structure.RecordCount)
		//	Read each record
		for i := 0; i < int(db_structure.RecordCount); i++ {
			var record Record
			// Reads filename [40 bytes]
			if err := binary.Read(file, binary.LittleEndian, &record.FileName); err != nil {
				log.Fatal("[MAIN] Error reading FileName: ", err)
			}
			// reads file size [8 bytes]
			if err := binary.Read(file, binary.LittleEndian, &record.Size); err != nil {
				log.Fatal("[MAIN] Error reading size: ", err)
			}
			// read records in order and send them to main db
			db_structure.Records = append(db_structure.Records, record)
		}
		// move cursor up to total record counts
		move_cursor(&db_structure.Records)
		fmt.Printf("[MAIN] %s has %d files and cursor position is at %d\n", file.Name(), db_structure.RecordCount, cursor_position)
		print_dbstat(&db_structure)

		repl(file, &db_structure)

	}
}

func print_dbstat(db *DatabaseStructure) {
	fmt.Println("----------------------")
	fmt.Println("ORD  Filename  Size")
	for ix, val := range db.Records {
		size := val.Size
		if size > (1024 * 1024) {
			// Convert size to MB
			sizeMB := float64(size) / (1024 * 1024)
			fmt.Printf("%-3d | %s | %.1f MiB\n", ix, val.FileName, sizeMB)
		} else if size > 1024 {
			sizeKB := float64(size) / 1024
			fmt.Printf("%-3d | %s | %.1f KiB\n", ix, val.FileName, sizeKB)
		} else {
			fmt.Printf("%-3d | %s | %d B\n", ix, val.FileName, size)
		}
	}
	fmt.Println("----------------------")
}

func print_help() {
	fmt.Println("\tread  	 <file>")
	fmt.Println("\treadio    <file>")
	fmt.Println("\twrite  	 <file> 	 <order|optional>")
	fmt.Println("\ttime	     code/<file> <times|optional>")
	fmt.Println("\tdelete 	 <file>")
	fmt.Println("\toptimize1")
	fmt.Println("\toptimize2")
	fmt.Println("\tclose OR exit")
}

func repl(file *os.File, db *DatabaseStructure) {
	scanner := bufio.NewScanner(os.Stdin)
ReadLoop:
	for {
		fmt.Print("> ")
		scanner.Scan()
		command := scanner.Text()

		if strings.HasPrefix(command, "readio") {
			args := strings.Split(command, " ")
			// read test.txt
			if len(args) != 2 {
				fmt.Println("open <filename>")
				continue ReadLoop
			}
			if !read(file, db, args[1], os.Stdout) {
				continue ReadLoop
			}
			write_readLog(os.Args[1], db, args[1]) // log to db.csv
		} else if strings.HasPrefix(command, "read") {
			args := strings.Split(command, " ")
			if len(args) != 2 {
				fmt.Println("open <filename>")
				continue ReadLoop
			}

			var buffer bytes.Buffer
			lenbefore := buffer.Len()

			start_opt := time.Now()
			if !opt2_flag {
				if !read(file, db, args[1], &buffer) {
					buffer.Reset()
					debug.FreeOSMemory()
					continue ReadLoop
				}
			} else {
				if dur := optimize_algo2(file, db, args[1], &buffer, nil); dur == 0 {
					buffer.Reset()
					debug.FreeOSMemory()
					continue ReadLoop
				}
			}
			end_opt := time.Now()
			dur_opt := end_opt.Sub(start_opt)

			write_readLog(os.Args[1], db, args[1])
			var file_size int64
			for _, rec := range db.Records {
				if byteReadable(rec.FileName) == args[1] {
					file_size = rec.Size
				}
			}
			if lenbefore != 0 || buffer.Len() < lenbefore || file_size != int64(buffer.Len()) {
				fmt.Printf("Inconsistency reading")
			}
			fmt.Printf("Time: %v\n", dur_opt)
			buffer.Reset()
			debug.FreeOSMemory()
		} else if strings.HasPrefix(command, "write") {
			args := strings.Split(command, " ")
			// write test.txt or write test.txt 3
			var order uint8 = db.RecordCount
			// place in database records
			if len(args) == 3 {
				// 3rd argument is order so convert into int
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
			args := strings.Split(command, " ")
			if len(args) != 2 {
				fmt.Println("delete <filename>")
				continue ReadLoop
			}
			core_delete(file, db, args[1])
		} else if strings.HasPrefix(command, "close") || strings.HasPrefix(command, "exit") {
			break ReadLoop
		} else if strings.HasPrefix(command, "stat") {
			print_dbstat(db)
		} else if strings.HasPrefix(command, "optimize1") {
			optimize_algo1(file, db, get_occurance_slice(db, os.Args[1])) // first opt, get files closer
		} else if strings.HasPrefix(command, "optimize2") { // second opt, caching next common
			if !opt2_flag {
				last_fileinfo = get_occurance_slice(db, os.Args[1])
				opt2_flag = true
				fmt.Println("[REPL] OPT2 turned on")
			} else {
				fmt.Println("[REPL] OPT2 turned off")
				opt2_flag = false
			}

		} else if strings.HasPrefix(command, "time") { // does a timed test
			args := strings.Split(command, " ")
			times := 1
			if len(args) == 3 {
				t_tim, err := strconv.Atoi(args[2])
				if err != nil {
					fmt.Println("time <filename> <times|optional>")
					continue ReadLoop
				}
				times = t_tim

			} else if len(args) != 2 {
				fmt.Println("time <filename> <times|optional>")
				continue ReadLoop
			}
			timed_execute(args[1], times)
		} else if strings.HasPrefix(command, "help") {
			print_help()
		} else {
			fmt.Println("Unknown command. Please use one of the following: ")
			print_help()
		}
	}
	file.Close()
}

func write_readLog(dbname string, db *DatabaseStructure, filename string) {
	/* READLOG
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
		// if file does not exist, exit
		return
	}

	// name of binary file "filename"
	csvPath := "./logs/" + logfilename(dbname)
	// name of csv file "./logs/filename.csv"
	// create logs folder if it doesn't exist
	_, err := os.Stat("./logs")
	if os.IsNotExist(err) {
		// Folder doesn't exist, create it
		err := os.Mkdir("./logs", 0755)
		if err != nil {
			fmt.Println("Error creating folder:", err)
			return
		}
	}

	_, err_stat := os.Stat(csvPath)

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
	// Check file's existance
	if os.IsNotExist(err_stat) {
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

func get_occurance_slice(db *DatabaseStructure, binaryName string) []EFilePair {
	csvPath := "./logs/" + logfilename(binaryName)
	records := read_readlog(csvPath)
	if records == nil {
		return nil // nothing to optimize
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
	if len(falgo_pslice) < 1 {
		// TODO: add error log
		fmt.Printf("[OPT] No algo to build")
		return nil
	}
	fmt.Println("------")
	for _, falgo := range falgo_pslice {
		fmt.Printf("%s (%d) -> %s\n", falgo.Fname, falgo.Info.TotalWeight, falgo.Info.MaxEdges[0])
	}
	fmt.Println("------")
	return falgo_pslice
}

func optimize_algo1(file *os.File, db *DatabaseStructure, falgo_pslice []EFilePair) {
	if falgo_pslice == nil {
		return
	}
	final_res := make([]string, 0)

	falgo := falgo_pslice[0]
	final_res = append(final_res, falgo.Fname) // first

	var next_falgo = ""
	if len(falgo.Info.MaxEdges) > 0 { // second
		next_falgo = falgo.Info.MaxEdges[0]
		if next_falgo != "" {
			final_res = append(final_res, next_falgo)
		}
	}

	var new_pairs []string = nil
MainLoop:
	for { // rest
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
	fmt.Printf("%+v\n", final_res)
	for _, value := range final_res {
		n_db = append(n_db, truncateString(value))
	}
	reorg(file, db, n_db)
}

// read with caching next
func optimize_algo2(file *os.File, db *DatabaseStructure, filename string, dst io.Writer, falgo_pslice []EFilePair) (dur time.Duration) {
	if falgo_pslice == nil {
		falgo_pslice = last_fileinfo
	}
	cold_read_request = true
	start_opt := time.Now()
	if !read(file, db, filename, dst) {
		return
	}
	end_opt := time.Now()
	cold_read_request = false
	//fmt.Printf("read %s\n", filename)
	if falgo_pslice == nil {
		return
	}
	var next_file = ""
	for _, val := range falgo_pslice {
		if filename == val.Fname {
			next_file = val.Info.MaxEdges[0]
			break
		}
	}
	if next_file != "" {
		//fmt.Printf("queued %s\n", next_file)
		IdleQueue.Enqueue(QueueRecord{FileName: next_file, SizeRead: 0})
		//go read_next(file, db, next_file)
	}
	return end_opt.Sub(start_opt)
}

var cold_read_request bool = false

const chunkSize = 1048576

func idle_loop(file *os.File, db *DatabaseStructure) {
	for {
		if IdleQueue.IsEmpty() || cold_read_request {
			continue
		}
		if DATABASE_LOCK.TryLock() {
			qitem, _ := IdleQueue.Peek()
			total_read, file_size := read_next(file, db, qitem.FileName, qitem.SizeRead)
			if total_read == file_size {
				IdleQueue.Dequeue()
			} else {
				IdleQueue.items[0].SizeRead = total_read
			}
			//fmt.Println("Mutex is unlocked")
			DATABASE_LOCK.Unlock()
		} else {
			time.Sleep(time.Second / 10)
		}
		//time.Sleep(1 * time.Second) // Sleep for a while before checking again
	}
}

func read_next(file *os.File, db *DatabaseStructure, next_file string, sizeread int64) (total_read int64, file_size int64) {
	// FROM CORE.READ //////////////
	// fail if we didn't write any files yet
	if db.RecordCount == 0 {
		fmt.Println("[NEXT] Database has no files written")
		return 0, file_size
	}
	// calculate the location of file in the database
	var location int64 = binary_size(Record{})*int64(db.RecordCount) + binary_size(&db.RecordCount)
	for r_count, record := range db.Records {
		if record_name_compare(record.FileName, next_file) {
			file_size = record.Size
			break
		}
		location += record.Size
		if r_count+1 == int(db.RecordCount) { // fail if you reached end
			fmt.Println("[NEXT] No such file in database")
			return 0, file_size
		}
	}
	/////////////////////////////
	location += sizeread // test this
	_, err := file.Seek(location, io.SeekStart)
	if err != nil {
		fmt.Printf("[NEXT] Error seeking the location: %v", err)
		return 0, file_size
	}

	var bytebuff = bytes.NewBuffer([]byte{})
	if buffy := file_buffer_map[next_file]; buffy != nil {
		if file_buffer_map[next_file].Len() == int(file_size) {
			return file_size, file_size
		}
	} else {
		file_buffer_map[next_file] = bytebuff
	}

	for {
		lcsize := chunkSize
		if chunkSize+file_buffer_map[next_file].Len() > int(file_size) {
			lcsize = int(file_size) - file_buffer_map[next_file].Len()
		}
		if lcsize == 0 {
			break
		}
		buf := make([]byte, lcsize)

		n, err := file.Read(buf)
		if n > 0 {
			file_buffer_map[next_file].Write(buf[:n])
			total_read += int64(n)
		}
		if err != nil {
			// test me
			break
		}
		if cold_read_request {
			//fmt.Printf("Cold read request detected. Added %s - %d to the buffer\n", next_file, file_buffer_map[next_file].Len())
			return int64(file_buffer_map[next_file].Len()), file_size
		}
	}

	if bytebuff.Len() == 0 {
		return 0, file_size //unsure
	}
	//fmt.Printf("Added %s - %d to the buffer\n", next_file, file_buffer_map[next_file].Len())
	return int64(file_buffer_map[next_file].Len()), file_size
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
