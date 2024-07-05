package main

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type BitCask struct {
	sync.RWMutex
	directory       string
	currentDataFile *DataFile
	keyDir          KeyDir
	dataFiles       map[string]*DataFile

	maxFileSize  int64
	maxDataFiles int // required ? if yes, how to handle key expiry functionality ?
	shouldFsync  bool
}

func NewBitCask(directory string) (*BitCask, error) {
	// create directory if not exist.
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	files, err := filepath.Glob(fmt.Sprintf("%s/*.db", directory))
	if err != nil {
		return nil, err
	}

	dataFiles := map[string]*DataFile{}
	index := 0
	if len(files) > 0 {
		for _, f := range files {
			_, fileName := filepath.Split(f)
			id := strings.TrimPrefix(fileName, DATA_FILE_PREFIX)
			df, err := NewDataFile(directory, id)
			if err != nil {
				return nil, err
			}
			dataFiles[id] = df
			idx, err := strconv.Atoi(id)
			if err != nil {
				return nil, err
			}

			if idx > index {
				index = idx
			}
		}
	}

	// use previous last index file or always create new file when initiated ? for now, create new file to keep it simple.
	newIdx := index + 1
	currentDataFile, err := NewDataFile(directory, fmt.Sprintf("%d", newIdx))
	if err != nil {
		return nil, err
	}

	keyDir := make(KeyDir)
	// Check if a keydir snapshot file already exists and then use that to populate the hashtable.
	// keyDir.Decode(<snapshot file location>)

	// TODO
	// initiate compact go routine

	return &BitCask{
		currentDataFile: currentDataFile,
		directory:       directory,
		keyDir:          keyDir,
		dataFiles:       dataFiles,
		maxFileSize:     100000,
		maxDataFiles:    10,
		shouldFsync:     true,
	}, nil
}

func (b *BitCask) Shutdown() error {
	b.Lock()
	defer b.Unlock()

	// snapshot key data.

	// Close all active file handlers.
	if err := b.currentDataFile.Close(); err != nil {
		log.Printf("failed to close the active db file %s: %v\n", b.currentDataFile.ID(), err)
		return err
	}

	// Close all other datafiles as well.
	for _, df := range b.dataFiles {
		if err := df.Close(); err != nil {
			log.Printf("failed to close the db file %s: %v\n", b.currentDataFile.ID(), err)
			return err
		}
	}

	return nil
}

func (b *BitCask) Put(key string, value []byte) error {
	b.Lock()
	defer b.Unlock()
	// validate key , value sizes etc?
	return b.put(key, value, nil)
}

func (b *BitCask) PutWithExpiry(key string, value []byte, d time.Duration) error {
	b.Lock()
	defer b.Unlock()
	// validate key , value sizes etc?

	ex := time.Now().Add(d)
	return b.put(key, value, &ex)
}

func (b *BitCask) Delete(key string) error {
	// delete from key
	// add entry to current data file with delete marker. if data files must be immutable, remove deleted entries in compact process.
	return nil
}

func (b *BitCask) Get(key string) ([]byte, error) {
	b.Lock()
	defer b.Unlock()
	r, err := b.get(key)
	if err != nil {
		return nil, err
	}

	if r.isExpired() {
		return nil, nil // return expired error ?
	}

	return r.Value, nil
}

func (b *BitCask) put(key string, val []byte, expiry *time.Time) error {
	// Prepare header.
	header := Header{
		Checksum:  crc32.ChecksumIEEE(val),
		Timestamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValSize:   uint32(len(val)),
	}

	// Check for expiry.
	if expiry != nil {
		header.Expiry = uint32(expiry.Unix())
	} else {
		header.Expiry = 0
	}

	// Prepare the record.
	record := Record{
		Header: header,
		Key:    key,
		Value:  val,
	}

	// TODO: Get the buffer from the pool for writing data.
	buf := &bytes.Buffer{}

	// Encode header.
	header.encode(buf)

	// Write key/value.
	buf.WriteString(key)
	buf.Write(val)

	if err := b.rotateLogIfNeeded(int64(buf.Len())); err != nil {
		return err
	}

	// Append to underlying file.
	offset, err := b.currentDataFile.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("failed to write data to file: %v", err)
	}

	// Add entry to KeyDir.
	// We just save the value of key and some metadata for faster lookups.
	// The value is only stored in disk.
	b.keyDir[key] = Meta{
		Timestamp:  int(record.Header.Timestamp),
		RecordSize: len(buf.Bytes()),
		RecordPos:  offset + len(buf.Bytes()), // to keep it simple, use just offsets which starting position ?
		FileID:     b.currentDataFile.ID(),
	}

	// Ensure filesystem's in memory buffer is flushed to disk.
	if b.shouldFsync {
		if err := b.currentDataFile.Sync(); err != nil {
			return fmt.Errorf("error syncing file to disk: %v", err)
		}
	}

	return nil
}

func (b *BitCask) rotateLogIfNeeded(newDataLen int64) error {
	size, err := b.currentDataFile.Size()
	if err != nil {
		return err
	}

	if size+newDataLen >= b.maxFileSize {
		if err := b.rotateLog(); err != nil {
			return err
		}
	}

	return nil
}

func (b *BitCask) rotateLog() error {
	if err := b.currentDataFile.Sync(); err != nil {
		return err
	}

	b.dataFiles[b.currentDataFile.ID()] = b.currentDataFile
	// close the current datafile file ?
	/*
		if err := b.currentDataFile.Close(); err != nil {
			return err
		}
	*/

	id, err := strconv.Atoi(b.currentDataFile.ID())
	if err != nil {
		return err
	}
	// TODO: purge old data file based on max data files. during purge, purges keys as well ?

	// create new segment and attach to wal.
	newDataFile, err := NewDataFile(b.directory, fmt.Sprintf("%d", id+1))
	if err != nil {
		return err
	}

	b.currentDataFile = newDataFile

	return nil
}

func (b *BitCask) get(key string) (Record, error) {
	// Check for entry in KeyDir.
	meta, ok := b.keyDir[key]
	if !ok {
		return Record{}, fmt.Errorf("no found")
	}

	// Set the current file ID as the default.
	reader := b.currentDataFile

	// is key file is current data file ?.
	if meta.FileID != b.currentDataFile.ID() {
		reader, ok = b.dataFiles[meta.FileID]
		if !ok {
			return Record{}, fmt.Errorf("data file %s of key %s is missing", meta.FileID, key)
		}
	}

	var header Header
	// Read the file with the given offset.
	data, err := reader.Read(meta.RecordPos, meta.RecordSize)
	if err != nil {
		return Record{}, fmt.Errorf("failed to read datafile: %v", err)
	}

	// Decode the header.
	if err := header.decode(data); err != nil {
		return Record{}, fmt.Errorf("failed to decode header: %v", err)
	}

	// Get the offset position in record to start reading the value from.
	// record is header + key + value ..
	valPos := meta.RecordSize - int(header.ValSize)
	// Read the value from the record.
	val := data[valPos:]

	record := Record{
		Header: header,
		Key:    key,
		Value:  val,
	}

	return record, nil
}
