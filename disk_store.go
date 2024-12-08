package caskdb

import (
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
	"time"
)

// DiskStore is a Log-Structured Hash Table as described in the BitCask paper. We
// keep appending the data to a file, like a log. DiskStorage maintains an in-memory
// hash table called KeyDir, which keeps the row's location on the disk.
//
// The idea is simple yet brilliant:
//   - Write the record to the disk
//   - Update the internal hash table to point to that byte offset
//   - Whenever we get a read request, check the internal hash table for the address,
//     fetch that and return
//
// KeyDir does not store values, only their locations.
//
// The above approach solves a lot of problems:
//   - Writes are insanely fast since you are just appending to the file
//   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
//     storage, there could be 2-3 disk seeks
//
// However, there are drawbacks too:
//   - We need to maintain an in-memory hash table KeyDir. A database with a large
//     number of keys would require more RAM
//   - Since we need to build the KeyDir at initialisation, it will affect the startup
//     time too
//   - Deleted keys need to be purged from the file to reduce the file size
//
// Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf
//
// DiskStore provides two simple operations to get and set key value pairs. Both key
// and value need to be of string type, and all the data is persisted to disk.
// During startup, DiskStorage loads all the existing KV pair metadata, and it will
// throw an error if the file is invalid or corrupt.
//
// Note that if the database file is large, the initialisation will take time
// accordingly. The initialisation is also a blocking operation; till it is completed,
// we cannot use the database.
//
// Typical usage example:
//
//		store, _ := NewDiskStore("books.db")
//	   	store.Set("othello", "shakespeare")
//	   	author := store.Get("othello")
type DiskStore struct {
	offset int
	hashmap map[string]KeyEntry
	file *os.File
}

func isFileExists(fileName string) bool {
	// how to check if a file exists in go
	// https://stackoverflow.com/a/12518877 
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

func (diskStore *DiskStore) readFromFile(fileName string) {
	// just for normal opening, no specific permissions needed to open
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("failed to open file, err : %v", err)
	}
	// schedule file to close at the end of the function, 
	// if not will be closed near after the Open() call
	defer file.Close()

	// go's loops do not require a cond, as long as we return/ break inside
	for {
		// make a bytearray of size 'headerSize'
		headerByteArray := make([]byte, headerSize)
		_, err := io.ReadFull(file, headerByteArray)
		if err == io.EOF {
			// exit condition
			break
		}

		if err != nil {
			log.Fatalf("failed to read from file while reading header, err := %v", err)
			break
		}

		timestamp, keySize, valSize := decodeHeader(headerByteArray)
		// use internal pointer to read forward
		key := make([]byte, keySize)
		_, err = io.ReadFull(file, key)
		if err != nil {
			log.Fatalf("failed to read from file while reading key, err := %v", err)
			break
		}
		val := make([]byte, valSize)
		_, err = io.ReadFull(file, val)
		if err != nil {
			log.Fatalf("failed to read from file while reading val, err := %v", err)
			break
		}

		totalSize := headerSize + keySize + valSize
		diskStore.hashmap[string(key)] = NewKeyEntry(timestamp, uint32(diskStore.offset), totalSize)
		diskStore.offset += int(totalSize)
	}
}

func NewDiskStore(fileName string) (*DiskStore, error) {
	diskStore := &DiskStore{hashmap: make(map[string]KeyEntry)}
	
	// if file exists, read from the file's contents and populate hashmap
	if isFileExists(fileName) {
		diskStore.readFromFile(fileName)
	} 
	
	// everyone can read write
	const FILE_PERMISSION = 0666
	file, err := os.OpenFile(fileName, os.O_APPEND | os.O_RDWR | os.O_CREATE, FILE_PERMISSION)
	if err != nil {
		log.Fatalf("failed to open/ create file, err: %v", err)
		return nil, err
	}
	diskStore.file = file
	return diskStore, nil
}

func (d *DiskStore) Get(key string) string {
	// get value from hashmap, in go lookups return a second param which is a bool
	keyEntry, ok := d.hashmap[key]
	if !ok {
		log.Printf("key of '%v' does not exist in diskstore!", key)
		return ""
	}

	buffer := d.read(int64(keyEntry.position), keyEntry.totalSize)
	_, decodedKey, value := decodeKV(buffer)
	if key != decodedKey {
		log.Fatalf("unmatched keys, requested %v but got %v", key, decodedKey)
	}
	return value
}

func (d *DiskStore) Set(key string, value string) {
	// setup kvp
	timestamp := uint32(time.Now().Unix())
	size, data := encodeKV(timestamp, key, value)
	
	// ensure persistent write without errors
	d.write(data)

	// set key and handle offset
	keyEntry := NewKeyEntry(timestamp, uint32(d.offset), uint32(size))
	d.hashmap[key] = keyEntry
	d.offset += size
}

func (d *DiskStore) Close() bool {
	// flush in memory buffer and actually write to disk
	// uses in memory buffer as writing to disk after every write is expensive
	if err := d.file.Sync(); err != nil {
		log.Fatalf("failed to flush in memory buffer to disk, error: %v", err)
		return false
	}
	if err := d.file.Close(); err != nil {
		log.Fatalf("failed to close file, error: %v", err)
		return false
	}
	return true
}

func (d *DiskStore) read(position int64, size uint32) []byte {
	const DEFAULT_WHENCE = 0
	// file offset, different from the write offset we maintain
	if _, err := d.file.Seek(position, DEFAULT_WHENCE); err != nil {
		log.Fatalf("failed to seek to position: %v, error: %v", position, err)
	}
	buffer := make([]byte, size)
	if _, err := io.ReadFull(d.file, buffer); err != nil {
		log.Fatalf("failed to read from file, error: %v", err)
	}
	return buffer
}

func (d *DiskStore) write(data []byte) {
	if _, err := d.file.Write(data); err != nil {
		log.Fatalf("failed to write to file, error: %v", err)
	}
}
