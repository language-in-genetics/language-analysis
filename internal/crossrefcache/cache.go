package crossrefcache

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

const (
	Version   uint32 = 1
	HeaderLen        = 64
	RecordLen        = 80
)

var magic = [16]byte{'C', 'R', 'D', 'O', 'I', 'C', 'A', 'C', 'H', 'E', '0', '0', '0', '1', 0, 0}

type Record struct {
	DOIHash         [32]byte
	TextFingerprint [32]byte
	WorkID          uint64
	WorkVersionID   uint64
}

func HashDOI(value string) [32]byte {
	return sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(value))))
}

func Sort(records []Record) {
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i].DOIHash[:], records[j].DOIHash[:]) < 0
	})
}

func Find(records []Record, doiHash [32]byte) (Record, bool) {
	index := sort.Search(len(records), func(i int) bool {
		return bytes.Compare(records[i].DOIHash[:], doiHash[:]) >= 0
	})
	if index < len(records) && bytes.Equal(records[index].DOIHash[:], doiHash[:]) {
		return records[index], true
	}
	return Record{}, false
}

func WriteFile(path string, records []Record) error {
	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(file, 8*1024*1024)

	if err := writeHeader(writer, uint64(len(records))); err != nil {
		file.Close()
		return err
	}
	var buf [RecordLen]byte
	for _, record := range records {
		encodeRecord(buf[:], record)
		if _, err := writer.Write(buf[:]); err != nil {
			file.Close()
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func ReadFile(path string) ([]Record, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReaderSize(file, 8*1024*1024)
	count, err := readHeader(reader)
	if err != nil {
		return nil, err
	}
	expectedSize := int64(HeaderLen) + int64(count)*RecordLen
	if info.Size() != expectedSize {
		return nil, fmt.Errorf("cache file size %d does not match header count %d size %d", info.Size(), count, expectedSize)
	}
	records := make([]Record, count)
	var buf [RecordLen]byte
	for i := range records {
		if _, err := io.ReadFull(reader, buf[:]); err != nil {
			return nil, err
		}
		records[i] = decodeRecord(buf[:])
	}
	return records, nil
}

func writeHeader(writer io.Writer, count uint64) error {
	var header [HeaderLen]byte
	copy(header[0:16], magic[:])
	binary.LittleEndian.PutUint32(header[16:20], Version)
	binary.LittleEndian.PutUint32(header[20:24], RecordLen)
	binary.LittleEndian.PutUint64(header[24:32], count)
	_, err := writer.Write(header[:])
	return err
}

func readHeader(reader io.Reader) (uint64, error) {
	var header [HeaderLen]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, err
	}
	if !bytes.Equal(header[0:16], magic[:]) {
		return 0, fmt.Errorf("not a Crossref DOI cache file")
	}
	version := binary.LittleEndian.Uint32(header[16:20])
	if version != Version {
		return 0, fmt.Errorf("unsupported cache version %d", version)
	}
	recordLen := binary.LittleEndian.Uint32(header[20:24])
	if recordLen != RecordLen {
		return 0, fmt.Errorf("unsupported cache record length %d", recordLen)
	}
	return binary.LittleEndian.Uint64(header[24:32]), nil
}

func encodeRecord(buf []byte, record Record) {
	copy(buf[0:32], record.DOIHash[:])
	copy(buf[32:64], record.TextFingerprint[:])
	binary.LittleEndian.PutUint64(buf[64:72], record.WorkID)
	binary.LittleEndian.PutUint64(buf[72:80], record.WorkVersionID)
}

func decodeRecord(buf []byte) Record {
	var record Record
	copy(record.DOIHash[:], buf[0:32])
	copy(record.TextFingerprint[:], buf[32:64])
	record.WorkID = binary.LittleEndian.Uint64(buf[64:72])
	record.WorkVersionID = binary.LittleEndian.Uint64(buf[72:80])
	return record
}
