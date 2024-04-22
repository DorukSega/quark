package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
)

func write(file *os.File, db *DatabaseStructure, filepath string, order uint8)(err error) {
	//	MARK: WRITE
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
		//log.Fatal("[WRITE] Temporary file failed to create ", err)
	}
	metadata_point := binary_size(Record{}) * int64(order)
	//	where to write file in record order

	// Write the first byte  to the file
	var first_byte uint8 = db.RecordCount + 1
	if err := binary.Write(tempFile, binary.LittleEndian, first_byte); err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("[WRITE] Failed to write new record count  %v", err)
		//log.Fatal("[WRITE] Failed to write new record count ", err)
	}

	//	Read data from the original file up to 
	//	the record insertion point and write it to the temporary file
	_, err = file.Seek(binary_size(first_byte), io.SeekStart)
	// file place to first_byte 
	if err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("[WRITE] Failed to seek start %v", err)
		// log.Fatal("[WRITE] Failed to seek start ", err)
	}

	_, err = io.CopyN(tempFile, file, metadata_point)
	// Copy until
	if err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("[WRITE] Failed to write the old metadata %v", err)
		// log.Fatal("[WRITE] Failed to write the old metadata ", err)
	}

	//fmt.Println("[WRITE] metadata_point: ", metadata_point)

	// Write the new record
	if err := binary.Write(tempFile, binary.LittleEndian, record.FileName); err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("[WRITE] Failed to write new record name %v", err)
		//log.Fatal("[WRITE] Failed to write new record name ", err)
	}
	if err := binary.Write(tempFile, binary.LittleEndian, record.Size); err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("[WRITE] Failed to write new record size %v", err)
		//log.Fatal("[WRITE] Failed to write new record size ", err)
	}

	// get rest
	left_record_point := binary_size(Record{})*int64(db.RecordCount) - metadata_point

	_, err = io.CopyN(tempFile, file, left_record_point)
	if err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("[WRITE] Failed to write the rest of metadata: %v", err)
		//log.Fatal("[WRITE] Failed to write the rest of metadata: ", err)
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
		os.Remove(tempFile.Name())
		return fmt.Errorf("[WRITE] Failed to write the files before: %v", err)
		//log.Fatal("[WRITE] Failed to write the files before: ", err)
	}

	// Write new file
	_, err = io.Copy(tempFile, new_file)
	if err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("[WRITE] Failed to write the new file %v", err)
		//log.Fatal("[WRITE] Failed to write the new file ", err)
	}

	// Read the remaining data from the original file and write it to the temporary file
	_, err = io.Copy(tempFile, file)
	if err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("[WRITE] Failed to write rest of the files %v", err)
		//log.Fatal("[WRITE] Failed to write rest of the files ", err)
	}

	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("[WRITE] Error going back to start in temp file %v", err)
		//log.Fatal("[WRITE] Error going back to start in temp file ", err)
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("[WRITE] Error going back to start in main file %v", err)
		//log.Fatal("[WRITE] Error going back to start in main file ", err)
	}

	_, err = io.Copy(file, tempFile)
	if err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("[WRITE] Failed to write back to database %v", err)
		//log.Fatal("[WRITE] Failed to write back to database ", err)
	}

	// get cursor pos
	n_seek, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		os.Remove(tempFile.Name())
		return fmt.Errorf("[WRITE] Error getting cursor position %v", err)
		//log.Fatal("[WRITE] Error getting cursor position ", err)
	}
	cursor_position = int64(n_seek)

	// Write new record in memory
	db.RecordCount += 1
	db.Records = append(db.Records, Record{})
	copy(db.Records[order+1:], db.Records[order:])
	db.Records[order] = record

	// Remove (delete) the temporary file
	tempFile.Close()
	err = os.Remove(tempFile.Name())
	if err != nil {
		return fmt.Errorf("[WRITE] Error removing temporary file: %v", err)
		//log.Fatal("[WRITE] Error removing temporary file:", err)
	}

	fmt.Println("[WRITE] Write complete")
	return nil
}

func read(file *os.File, db *DatabaseStructure, filename string, dst io.Writer) (err error) {
	// fail if we didn't write any files yet
	if db.RecordCount == 0 {
		fmt.Println("[READ] Database has no files")
		return
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
			return
		}
	}
	// seek to the location
	//fmt.Println("[READ] Read location for debug purposes", location)

	new_offset, err := file.Seek(location, io.SeekStart)
	if err != nil {
		return fmt.Errorf("[READ] Error seeking the location: %v", err)
		//log.Fatal("[READ] Error seeking the location ", err)
	}

	// read and write to custom writer interface, quite often the stdout
	fmt.Println("[READ START]")

	_, err = io.CopyN(dst, file, file_size)
	if err != nil {
		return fmt.Errorf("[READ] Failed reading file and writing to custom io: %v", err)
		//log.Fatal("[READ] Failed reading file and writing to custom io: ", err)
	}
	cursor_position = new_offset + file_size

	fmt.Printf("\n[READ END]\n")
	return nil
}

func delete(file *os.File, db *DatabaseStructure, filename string) {
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
		fmt.Println("[WRITE] Order is unusable")
		return
	}
	// Create a temporary file for writing
	tempFile, err := os.CreateTemp("./", "tempfile")
	if err != nil {
		log.Fatal("[Delete] Temporary file failed to create ", err)
	}
	// Write the first byte to the file
	var first_byte uint8 = db.RecordCount - 1
	metadata_point := binary_size(Record{})*int64(db.RecordCount) + binary_size(first_byte)
	if err := binary.Write(tempFile, binary.LittleEndian, first_byte); err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[Delete] Failed to write new record count ", err)
	}
	// Read data from the original file up to the record insertion point and write it to the temporary file
	_, err = file.Seek(binary_size(first_byte), io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[Delete] Failed to seek start ", err)
	}

	//fmt.Println("[Delete] metadata_point: ", metadata_point)

	for i := 0; i < int(db.RecordCount); i++ {
		if i == int(order) {
			continue
		} else {
			// Write the new record
			if err := binary.Write(tempFile, binary.LittleEndian, db.Records[i].FileName); err != nil {
				os.Remove(tempFile.Name())
				log.Fatal("[Delete] Failed to write new record name ", err)
			}
			if err := binary.Write(tempFile, binary.LittleEndian, db.Records[i].Size); err != nil {
				os.Remove(tempFile.Name())
				log.Fatal("[Delete] Failed to write new record size ", err)
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
		log.Fatal("[Delete] Error skipping mistake ", err)
	}

	//fmt.Println("[Delete] insertion point: ", insertion_point)

	_, err = io.CopyN(tempFile, file, insertion_point)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[Delete] Failed to write the files before: ", err)
	}

	/// OKAY
	_, err = file.Seek(location, 0)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[Delete] Error skipping mistake ", err)
	}

	// Read the remaining data from the original file and write it to the temporary file
	_, err = io.Copy(tempFile, file)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[Delete] Failed to write rest of the files ", err)
	}

	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[Delete] Error going back to start in temp file ", err)
	}

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[Delete] Error going back to start in main file ", err)
	}

	//// STOP TOO LATE
	_, err = io.Copy(file, tempFile)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[Delete] Failed to write back to database ", err)
	}
	tempFileSize, err := tempFile.Seek(0, io.SeekEnd)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[Delete] Error getting size of temp file ", err)
	}

	// Truncate the original file to match the size of the temporary file
	err = file.Truncate(tempFileSize)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[Delete] Failed to truncate main file ", err)
	}

	// get cursor pos
	n_seek, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[Delete] Error getting cursor position ", err)
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
	// Create a temporary file for writing
	tempFile, err := os.CreateTemp("./", "tempfile")
	if err != nil {
		log.Fatal("[REORG] Temporary file failed to create ", err)
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
		log.Fatal("[REORG] Failed to write new record count ", err)
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
			log.Fatal("[REORG] Failed to write the new metadata ", err)
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

		_, err = file.Seek(metadata_end+file_pos, io.SeekStart)
		if err != nil {
			os.Remove(tempFile.Name())
			log.Fatal("[REORG] Failed to seek file ", err)
		}

		_, err = io.CopyN(tempFile, file, nrecord.Size)
		if err != nil {
			os.Remove(tempFile.Name())
			log.Fatal("[REORG] Failed to write the file: ", err)
		}
	}

	// replace DatabaseStructure with new one
	*db = new_db
	// replace file with temp
	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[REORG] Error going back to start in temp file ", err)
	}
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[REORG] Error going back to start in main file ", err)
	}

	_, err = io.Copy(file, tempFile)
	if err != nil {
		os.Remove(tempFile.Name())
		log.Fatal("[REORG] Failed to write back to database ", err)
	}
	// TODO: get cursor post
	tempFile.Close()
	err = os.Remove(tempFile.Name())
	if err != nil {
		log.Fatal("[REORG] Error removing temporary file:", err)
	}

	fmt.Println("[REORG] Write complete")
}
