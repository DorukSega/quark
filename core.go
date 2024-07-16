package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
	"os"
	"runtime/debug"
	"time"
)

func write(file *os.File, db *DatabaseStructure, filepath string, order uint8) (err error) {
	DATABASE_LOCK.Lock()
	defer DATABASE_LOCK.Unlock()

	if order > db.RecordCount {
		fmt.Println("[WRITE] Order is unusable")
		return
	}

	// open file
	new_file, err := os.Open(filepath)
	if err != nil {
		fmt.Println("[WRITE] Error opening source file: ", err)
		return
	}
	defer new_file.Close()

	fileInfo, err := new_file.Stat()
	if err != nil {
		fmt.Println("[Write] Can't read file ", err)
		return
	}
	file_size := fileInfo.Size()
	file_name := fileInfo.Name()
	if record_contains(db, file_name) {
		fmt.Println("[Write] File already exists", file_name)
		return
	}
	// Create Record
	var record Record
	record.FileName = truncateString(file_name)
	record.Size = file_size

	fmt.Printf("[WRITE] Writing %s at %d with size %d\n", record.FileName, order, record.Size)

	// Create a temporary file for writing
	tempFile, err := os.CreateTemp("./", "tempfile")
	if err != nil {
		return fmt.Errorf("[WRITE] Temporary file failed to create  %v", err)
	}
	defer os.Remove(tempFile.Name())
	metadata_point := binary_size(Record{}) * int64(order)
	//	where to write file in record order

	// Write the first byte  to the file
	var first_byte uint8 = db.RecordCount + 1
	if err := binary.Write(tempFile, binary.LittleEndian, first_byte); err != nil {
		return fmt.Errorf("[WRITE] Failed to write new record count  %v", err)
	}

	//	Read data from the original file up to
	//	the record insertion point and write it to the temporary file
	_, err = file.Seek(binary_size(first_byte), io.SeekStart)
	// file place to first_byte
	if err != nil {
		return fmt.Errorf("[WRITE] Failed to seek start %v", err)
	}

	_, err = io.CopyN(tempFile, file, metadata_point)
	// Copy until
	if err != nil {
		return fmt.Errorf("[WRITE] Failed to write the old metadata %v", err)
	}

	//fmt.Println("[WRITE] metadata_point: ", metadata_point)

	// Write the new record
	if err := binary.Write(tempFile, binary.LittleEndian, record.FileName); err != nil {
		return fmt.Errorf("[WRITE] Failed to write new record name %v", err)
	}
	if err := binary.Write(tempFile, binary.LittleEndian, record.Size); err != nil {
		return fmt.Errorf("[WRITE] Failed to write new record size %v", err)
	}

	// get rest
	left_record_point := binary_size(Record{})*int64(db.RecordCount) - metadata_point

	_, err = io.CopyN(tempFile, file, left_record_point)
	if err != nil {
		return fmt.Errorf("[WRITE] Failed to write the rest of metadata: %v", err)
	}

	//fmt.Println("[WRITE] left_record_point: ", left_record_point)

	// insertion point
	var insertion_point int64 = 0
	for i := 0; i < int(order); i++ {
		insertion_point += db.Records[i].Size
	}

	//fmt.Println("[WRITE] insertion point: ", insertion_point)

	// Read data from the original file up to the file insertion point and write it to the temporary file
	_, err = io.CopyN(tempFile, file, insertion_point)
	if err != nil {
		return fmt.Errorf("[WRITE] Failed to write the files before: %v", err)
	}

	// Write new file
	_, err = io.Copy(tempFile, new_file)
	if err != nil {
		return fmt.Errorf("[WRITE] Failed to write the new file %v", err)
	}

	// Read the remaining data from the original file and write it to the temporary file
	_, err = io.Copy(tempFile, file)
	if err != nil {
		return fmt.Errorf("[WRITE] Failed to write rest of the files %v", err)
	}

	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("[WRITE] Error going back to start in temp file %v", err)
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("[WRITE] Error going back to start in main file %v", err)
	}

	_, err = io.Copy(file, tempFile)
	if err != nil {
		return fmt.Errorf("[WRITE] Failed to write back to database %v", err)
	}

	// get cursor pos
	n_seek, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("[WRITE] Error getting cursor position %v", err)
	}
	cursor_position = int64(n_seek)

	// Write new record in memory
	db.RecordCount += 1
	db.Records = append(db.Records, Record{})
	copy(db.Records[order+1:], db.Records[order:])
	db.Records[order] = record

	// Remove (delete) the temporary file
	tempFile.Close()

	fmt.Println("[WRITE] Write complete")
	return nil
}

func read(file *os.File, db *DatabaseStructure, filename string, dst io.Writer) (successful bool) {
	DATABASE_LOCK.Lock()
	defer DATABASE_LOCK.Unlock()

	// fail if we didn't write any files yet
	if db.RecordCount == 0 {
		fmt.Println("[READ] Database has no files written")
		return false
	}
	var file_size int64 = 0
	// calculate the location of file in the database
	var location int64 = binary_size(Record{})*int64(db.RecordCount) + binary_size(&db.RecordCount)
	for r_count, record := range db.Records {
		if record_name_compare(record.FileName, filename) {
			file_size = record.Size
			break
		}
		location += record.Size
		if r_count+1 == int(db.RecordCount) { // fail if you reached end
			// Todo: read fail case, should be something that programs can understand
			fmt.Println("[READ] No such file in database")
			return false
		}
	}

	if buff := file_buffer_map[filename]; buff != nil {
		reader := bytes.NewReader(buff.Bytes())
		if int64(reader.Len()) == file_size {
			_, err := io.Copy(dst, reader)
			if err != nil {
				fmt.Printf("[READ] Failed reading from buffer: %v", err)
				return false
			}
			//fmt.Printf("full in cache %s\n", filename)
			return true
		} else {
			_, err := io.Copy(dst, reader)
			if err != nil {
				fmt.Printf("[READ] Failed reading from buffer: %v", err)
				return false
			}
			relen := reader.Size()
			location += int64(relen)
			file_size -= int64(relen)
			//fmt.Printf("some in cache %s - %d\n", filename, relen)
		}
	}

	// seek to the location
	//fmt.Println("[READ] Read location for debug purposes", location)

	_, err := file.Seek(location, io.SeekStart)
	if err != nil {
		fmt.Printf("[READ] Error seeking the location: %v", err)
		return false
	}
	//cursor_position = new_offset + file_size

	// read and write to custom writer interface

	_, err = io.CopyN(dst, file, file_size)
	if err != nil {
		fmt.Printf("[READ] Failed reading file: %v", err)
		return false
	}
	return true
}

func core_delete(file *os.File, db *DatabaseStructure, filename string) {
	DATABASE_LOCK.Lock()
	defer DATABASE_LOCK.Unlock()
	// check if database has any file
	if db.RecordCount == 0 {
		fmt.Println("[DELETE] Database has no files")
		return
	}

	var file_size int64 = 0
	// file_size: data size of that file in test.bin
	var location int64 = binary_size(Record{})*int64(db.RecordCount) + binary_size(&db.RecordCount)
	// location: location of that file in test.bin
	var order uint8 = 0
	// record_order: order of record in all records
	for r_count, record := range db.Records {
		if record_name_compare(record.FileName, filename) {
			file_size = record.Size
			break
		}
		location += record.Size
		order += 1
		if r_count+1 == int(db.RecordCount) { // fail if you reached end
			// Todo: read fail case, should be something that programs can understand
			fmt.Println("[DELETE] No such file in database")
			return
		}
	}
	location += file_size
	// seek to the location

	//fmt.Println("[Delete] Delete location for debug purposes", location)
	if order > db.RecordCount {
		fmt.Println("[DELETE] Order is unusable")
		return
	}
	// Create a temporary file for writing
	tempFile, err := os.CreateTemp("./", "tempfile")
	if err != nil {
		fmt.Println("[Delete] Temporary file failed to create ", err)
		return
	}
	// Write the first byte to the file
	var first_byte uint8 = db.RecordCount - 1
	metadata_point := binary_size(Record{})*int64(db.RecordCount) + binary_size(first_byte)
	if err := binary.Write(tempFile, binary.LittleEndian, first_byte); err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[Delete] Failed to write new record count ", err)
		return
	}
	// Read data from the original file up to the record insertion point and write it to the temporary file
	_, err = file.Seek(binary_size(first_byte), io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[Delete] Failed to seek start ", err)
		return
	}

	//fmt.Println("[Delete] metadata_point: ", metadata_point)

	for i := 0; i < int(db.RecordCount); i++ {
		if i == int(order) {
			continue
		} else {
			// Write the new record
			if err := binary.Write(tempFile, binary.LittleEndian, db.Records[i].FileName); err != nil {
				os.Remove(tempFile.Name())
				fmt.Println("[Delete] Failed to write new record name ", err)
				return
			}
			if err := binary.Write(tempFile, binary.LittleEndian, db.Records[i].Size); err != nil {
				os.Remove(tempFile.Name())
				fmt.Println("[Delete] Failed to write new record size ", err)
				return
			}
		}
	}
	// insertion point
	var insertion_point int64 = 0
	for i := 0; i < int(order); i++ {
		insertion_point += db.Records[i].Size
	}
	_, err = file.Seek(metadata_point, 0)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[Delete] Error skipping mistake ", err)
		return
	}

	//fmt.Println("[Delete] insertion point: ", insertion_point)

	_, err = io.CopyN(tempFile, file, insertion_point)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[Delete] Failed to write the files before: ", err)
		return
	}

	/// OKAY
	_, err = file.Seek(location, 0)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[Delete] Error skipping mistake ", err)
		return
	}

	// Read the remaining data from the original file and write it to the temporary file
	_, err = io.Copy(tempFile, file)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[Delete] Failed to write rest of the files ", err)
		return
	}

	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[Delete] Error going back to start in temp file ", err)
		return
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[Delete] Error going back to start in main file ", err)
		return
	}

	//// STOP TOO LATE
	_, err = io.Copy(file, tempFile)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[Delete] Failed to write back to database ", err)
		return
	}
	tempFileSize, err := tempFile.Seek(0, io.SeekEnd)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[Delete] Error getting size of temp file ", err)
		return
	}

	// Truncate the original file to match the size of the temporary file
	err = file.Truncate(tempFileSize)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[Delete] Failed to truncate main file ", err)
		return
	}

	// get cursor pos
	n_seek, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[Delete] Error getting cursor position ", err)
		return
	}
	cursor_position = int64(n_seek)

	// Remove the last record from memory
	db.RecordCount -= 1
	copy(db.Records[order:], db.Records[order+1:])
	db.Records = db.Records[:len(db.Records)-1]

	// Remove (delete) the temporary file
	tempFile.Close()
	err = os.Remove(tempFile.Name())
	if err != nil {
		log.Fatal("Error removing temporary file:", err)
	}

	fmt.Println("[Delete] Delete complete")

}

func reorg(file *os.File, db *DatabaseStructure, new_rec [][40]byte) {
	// TODO: check if structure is same as before
	DATABASE_LOCK.Lock()
	defer DATABASE_LOCK.Unlock()
	// Create a temporary file for writing
	tempFile, err := os.CreateTemp("./", "tempfile")
	if err != nil {
		fmt.Println("[REORG] Temporary file failed to create ", err)
		return
	}
	new_db := DatabaseStructure{
		RecordCount: db.RecordCount,
		Records:     []Record{},
	}
	for _, n_filename := range new_rec {
		var n_size int64 = 0
		for _, val := range db.Records {
			if val.FileName == n_filename {
				n_size = val.Size
				break
			}
		}
		if n_size == 0 {
			fmt.Printf("[REORG] %s file not part of db\n", byteReadable(n_filename))
			os.Remove(tempFile.Name())
			return
		}
		new_db.Records = append(new_db.Records, Record{
			FileName: n_filename,
			Size:     n_size,
		})
	}

	// write new metadata
	var first_byte uint8 = new_db.RecordCount
	if err := binary.Write(tempFile, binary.LittleEndian, first_byte); err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[REORG] Failed to write new record count ", err)
		return
	}

	for _, record := range new_db.Records {
		// Convert the record struct to bytes
		data := make([]byte, 40+8) // 40 bytes for FileName + 8 bytes for Size
		copy(data[:40], record.FileName[:])
		binary.LittleEndian.PutUint64(data[40:], uint64(record.Size))

		// Write the bytes to the file
		_, err := tempFile.Write(data)
		if err != nil {
			os.Remove(tempFile.Name())
			fmt.Println("[REORG] Failed to write the new metadata ", err)
			return
		}
	}
	metadata_end := binary_size(Record{})*int64(db.RecordCount) + binary_size(first_byte)

	// write files one by one
	for _, nrecord := range new_db.Records {
		var file_pos int64 = 0
		for _, val := range db.Records {
			if val.FileName == nrecord.FileName {
				break
			}
			file_pos += val.Size
		}

		_, err := file.Seek(metadata_end+file_pos, io.SeekStart)
		if err != nil {
			os.Remove(tempFile.Name())
			fmt.Println("[REORG] Failed to seek file ", err)
			return
		}

		_, err = io.CopyN(tempFile, file, nrecord.Size)
		if err != nil {
			os.Remove(tempFile.Name())
			fmt.Println("[REORG] Failed to write the file: ", err)
			return
		}
	}

	// replace DatabaseStructure with new one
	*db = new_db
	// replace file with temp
	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[REORG] Error going back to start in temp file ", err)

		return
	}
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[REORG] Error going back to start in main file ", err)
		return
	}

	_, err = io.Copy(file, tempFile)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[REORG] Failed to write back to database ", err)
		return
	}
	// TODO: get cursor post
	n_seek, err := file.Seek(0, io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		fmt.Println("[REORG] Error going back to start in main file ", err)
		return
	}
	cursor_position = int64(n_seek)

	tempFile.Close()
	err = os.Remove(tempFile.Name())
	if err != nil {
		fmt.Println("[REORG] Error removing temporary file:", err)
		return
	}

	fmt.Println("[REORG] Reorganise complete")
	print_dbstat(db)
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
	go idle_loop(file, &db)

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
	var opt_state = 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if line == "WRITE" {
			flag = 1
			continue
		} else if line == "OPTIMIZE1" { // 1 -> Frequent-Neighbours
			opt_state = 1
			break
		} else if line == "OPTIMIZE2" { // 2 -> Next-Potential-Caching
			opt_state = 2
			break
		} else if line == "OPTIMIZE3" { // 3 -> Markov-Chain-Caching
			opt_state = 3
			fmt.Printf("NOT IMPLEMENTED YET")
			return
		} else if line == "OPTIMIZE" { // 4 -> ALL
			opt_state = 4
			fmt.Printf("NOT IMPLEMENTED YET")
			return
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
			if !read(file, &db, fname, buffer) {
				continue
			}
			end_unopt = time.Now()
			buffer.Reset()
			buffer = bytes.NewBuffer([]byte{1})
			debug.FreeOSMemory()
			if i == 0 {
				write_readLog(db_name, &db, fname)
			}
			dur_unopt += end_unopt.Sub(start_unopt)
		}
	}

	buffer = bytes.NewBuffer([]byte{2})
	buffer.Reset()

	debug.FreeOSMemory()
	var occurance_slice []EFilePair
	if opt_state == 1 {
		occurance_slice = get_occurance_slice(&db, db_name)
		optimize_algo1(file, &db, occurance_slice)
		fmt.Println("-- Frequent-Neighbours Optimization --")
	} else if opt_state == 2 {
		occurance_slice = get_occurance_slice(&db, db_name)
		fmt.Println("-- Next-Potential-Caching Optimization --")
	}

	debug.FreeOSMemory()

	var dur_opt time.Duration
	var start_opt time.Time
	var end_opt time.Time
	for i := 0; i < n; i++ {
		for _, fname := range to_read {
			var pdur_opt time.Duration
			if opt_state == 2 {
				pdur_opt = optimize_algo2(file, &db, fname, buffer, occurance_slice)
				dur_opt += pdur_opt
			} else {
				start_opt = time.Now()
				if !read(file, &db, fname, buffer) {
					continue
				}
				end_opt = time.Now()
				dur_opt += end_opt.Sub(start_opt)
			}
			if opt_state == 2 {
				// time wait, added to simulate a real usage,
				// where caching will have time to catch up
				// random duration between 100ms (0.1s) and 1s
				min := 100 * time.Millisecond
				max := 1 * time.Second
				randomDuration := min + time.Duration(mrand.Int63n(int64(max-min)))
				time.Sleep(randomDuration)
			}
			buffer.Reset()
			buffer = bytes.NewBuffer([]byte{2})
			debug.FreeOSMemory()

		}
	}

	buffer.Reset()

	fmt.Printf("[TIME] Before Optimization: %v\n", dur_unopt)
	fmt.Printf("[TIME] After Optimization: %v\n", dur_opt)
	fmt.Printf("[TIME]  %d%% Faster\n", (((dur_unopt - dur_opt) * 100) / dur_unopt))

	file.Close()
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
	for _, fpath := range to_write {
		err = os.Remove(fpath)
		if err != nil {
			fmt.Printf("[TIMED] can't remove file: %v\n", err)
			return
		}
	}
	debug.FreeOSMemory()
}
