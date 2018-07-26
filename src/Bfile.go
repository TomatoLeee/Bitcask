package bitcask

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync/atomic"
)

const RECORD_HEADER_SIZE = 20

type BFile struct {
	f      *os.File
	offset int64
	id     uint32
	maxSz  int64
}

func NewBfile(file *os.File, fileId uint32) (*BFile, error) {
	fi, err := file.Stat()

	if err != nil {
		return nil, err
	}
	offset := fi.Size()

	return &BFile{
		f:      file,
		offset: offset,
		id:     fileId,
	}, nil
}

//type Record struct {
//	crc    uint32 //4
//	tstamp int64  // 8
//	ksz    uint32 // 4
//	vsz    uint32 // 4
//	key    string
//	value  string
//}

//test
type Record struct {
	crc    uint32 //4
	tstamp int64  // 8
	ksz    uint32 // 4
	vsz    uint32 // 4
	key    []byte
	value  []byte
}

func (self *BFile) WriteHintRecord(hR *hintRecord) (int64, error) {
	//fmt.Println(hR.key)

	buf, err := hR.transfer()
	if err != nil {
		return -1, err
	}

	sz, err := self.f.Write(buf)

	if err != nil {
		return -1, err
	}

	if sz < len(buf) {
		return -1, fmt.Errorf("Read : exptectd %d, got %d", len(buf), sz)
	}

	self.offset += int64(sz)

	return self.offset, nil
}

//test
func (self *BFile) WriteRecord(key, value []byte, tstamp int64) (int64, error) {
	r := &Record{
		tstamp: tstamp,
		ksz:    uint32(len(key)),
		vsz:    uint32(len(value)),
		key:    key,
		value:  value,
	}

	buf, err := r.transfer()

	if err != nil {
		return -1, err
	}

	sz, err := self.f.Write(buf)
	if err != nil {
		return -1, err
	}

	if sz < len(buf) {
		return -1, fmt.Errorf("Read : exptectd %d, got %d", len(buf), sz)
	}

	//offset of value
	//fix
	voffset := self.offset + RECORD_HEADER_SIZE + int64(r.ksz)
	self.offset += int64(sz)
	return int64(voffset), nil
}

//func (self *BFile) WriteRecord(key, value string, tstamp int64) (int64, error) {
//	r := &Record{
//		tstamp: tstamp,
//		ksz:    uint32(len(key)),
//		vsz:    uint32(len(value)),
//		key:    key,
//		value:  value,
//	}
//
//	buf, err := r.transfer()
//
//	if err != nil {
//		return -1, err
//	}
//
//	sz, err := self.f.Write(buf)
//	if err != nil {
//		return -1, err
//	}
//
//	if sz < len(buf) {
//		return -1, fmt.Errorf("Read : exptectd %d, got %d", len(buf), sz)
//	}
//
//	//offset of value
//	//fix
//	voffset := self.offset + RECORD_HEADER_SIZE + int64(r.ksz)
//	self.offset += int64(sz)
//	return int64(voffset), nil
//}

//将Record转化为[]byte,并计算crc

func (self *Record) transfer() ([]byte, error) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, self.tstamp)
	binary.Write(buf, binary.BigEndian, self.ksz)
	binary.Write(buf, binary.BigEndian, self.vsz)
	buf.Write(self.key)
	buf.Write(self.value)

	crc := crc32.ChecksumIEEE(buf.Bytes())
	self.crc = crc
	buf2 := new(bytes.Buffer)
	binary.Write(buf2, binary.BigEndian, crc)
	buf2.Write(buf.Bytes())

	return buf2.Bytes(), nil
}

//func (self *Record) transfer() ([]byte, error) {
//	buf := new(bytes.Buffer)
//	binary.Write(buf, binary.BigEndian, self.tstamp)
//	//fmt.Printf("tstamp: %d\n", len(buf.Bytes()))
//	binary.Write(buf, binary.BigEndian, self.ksz)
//	//fmt.Printf("ksz: %d\n", len(buf.Bytes()))
//	binary.Write(buf, binary.BigEndian, self.vsz)
//	//fmt.Printf("vsz: %d\n", len(buf.Bytes()))
//	buf.Write([]byte(self.key))
//	//fmt.Printf("key: %d\n", len(buf.Bytes()))
//	//fmt.Println([]byte(key))
//	buf.Write([]byte(self.value))
//	//fmt.Printf("value: %d\n", len(buf.Bytes()))
//	//fmt.Println([]byte(value))
//
//	crc := crc32.ChecksumIEEE(buf.Bytes())
//	self.crc = crc
//	buf2 := new(bytes.Buffer)
//	binary.Write(buf2, binary.BigEndian, crc)
//	buf2.Write(buf.Bytes())
//
//	return buf2.Bytes(), nil
//}

// get value from hashmap  indexer
func (self *BFile) ReadRecord(i *Indexer) (string, error) {
	//header := make([]byte,Record_HEADER_SIZE)
	//	 //
	//	 //sz,err := self.f.Read()
	//fmt.Printf("vsz: %d\n", i.vsz)
	v := make([]byte, i.vsz)
	//fmt.Println(len(v))
	//fmt.Printf("voffset: %d\n", i.voffset)
	self.f.ReadAt(v, i.voffset)

	//fmt.Println(v)

	return string(v[:]), nil
}

func (self *BFile) Close() error {
	err := self.f.Close()
	return err
}

func (self *BFile) move(offset int64) error {
	//fix
	if self.offset+offset > self.maxSz {
		return fmt.Errorf("offset out of range")
	}

	self.f.Seek(offset, 0)
	atomic.StoreInt64(&self.offset, offset)
	return nil
}

//Read a Record
func (self *BFile) Next() (*Record, error) {
	r := new(Record)

	header := make([]byte, RECORD_HEADER_SIZE)

	sz, err := self.f.Read(header)

	if err != nil {
		return nil, err
	}

	if sz != RECORD_HEADER_SIZE {
		return nil, fmt.Errorf("Read Header: exptectd %d, got %d", RECORD_HEADER_SIZE, sz)
	}

	//decode
	reader := bytes.NewReader(header)

	binary.Read(reader, binary.BigEndian, &r.crc)
	binary.Read(reader, binary.BigEndian, &r.tstamp)
	binary.Read(reader, binary.BigEndian, &r.ksz)
	binary.Read(reader, binary.BigEndian, &r.vsz)

	//fmt.Println("next buf: %v",header)

	key := make([]byte, r.ksz)
	value := make([]byte, r.vsz)

	if _, err := self.f.Read(key); err != nil {
		return nil, err
	}

	if _, err := self.f.Read(value); err != nil {
		return nil, err
	}

	//r.key = string(key)
	//r.value = string(value)
	//test
	r.key = key
	r.value = value

	//fmt.Println("next key: %s",key)
	//fmt.Println("next value: %s",value)

	//to do
	//check crc
	header = append(header, key...)
	header = append(header, value...)

	//clear crc
	nCrc := crc32.ChecksumIEEE(header[4:])

	if r.crc != nCrc {
		fmt.Println(r.crc)
		fmt.Println(nCrc)
		return nil, fmt.Errorf("crc check error")
	}

	//fmt.Println("Next r: %V",r)

	return r, nil
}
