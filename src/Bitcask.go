package bitcask

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const testDathPath = "./data/"
const testHintPath = "./hint/"
const testOldDataPath = "./olddata/"

const testFileNAME = "./test"

type Bitcask struct {
	l             sync.RWMutex
	acFile        *BFile
	hintFile      *BFile
	mergeDataFile *BFile
	kv            *HashMap

	maxFileSize uint32

	data_path string
	hint_path string
	//merge_path string
}

func NewBitcask() *Bitcask {
	//test
	dirfp, err := os.OpenFile(testDathPath, os.O_RDONLY, 0766)
	if err != nil {
		fmt.Println(err)
	}

	defer dirfp.Close()

	flieNamelist, err := dirfp.Readdirnames(-1)

	if err != nil {
		fmt.Println(err)
	}

	var fileList []string

	for _, v := range flieNamelist {
		if strings.Contains(v, ".data") {
			fileList = append(fileList, v)
		}
	}

	var maxId int

	if fileList == nil {
		maxId = 0
	} else {
		sort.Strings(fileList)
		max := fileList[len(fileList)-1]
		index := strings.LastIndex(max, ".data")
		maxId, _ = strconv.Atoi(max[:index])
	}

	f, _ := os.OpenFile(testDathPath+strconv.Itoa(maxId+1)+".data", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0766)
	bf, _ := NewBfile(f, uint32(maxId+1))

	hashmap := NewHashMap()

	b := &Bitcask{
		acFile:      bf,
		kv:          hashmap,
		maxFileSize: 50,
		data_path:   testDathPath,
		hint_path:   testHintPath,
		//merge_path:  testOldDataPath,
	}

	return b
}

func (self *Bitcask) Get(key string) (string, error) {
	index, err := self.kv.Get(key)

	if err != nil {
		fmt.Println(err)
		return "", err
	}

	ret, err := self.get(index)
	if err != nil {
		//test
		fmt.Println(err)
		return "", err
	}

	return ret, nil
}

func (self *Bitcask) get(i *Indexer) (string, error) {
	fId := i.fId

	if fId == self.acFile.id {
		ret, err := self.acFile.ReadRecord(i)

		if err != nil {
			return "", err
		}

		return ret, nil
	} else {
		//fix
		// find data from old data file
		filePath := self.data_path + strconv.Itoa(int(fId)) + ".data"
		nFp, err := os.OpenFile(filePath, os.O_RDWR, 0766)
		if err != nil {
			return "", err
		}

		nBf, err := NewBfile(nFp, fId)
		defer nBf.Close()

		if err != nil {
			return "", err
		}

		ret, err := nBf.ReadRecord(i)

		if err != nil {
			return "", err
		}
		return ret, nil
	}

}

//type Indexer struct {
//	fId     uint32 //4
//	tstamp  int64  //8
//	vsz     uint32 //4
//	voffset int64  //8
//}
func (self *Bitcask) Set(key, value string) error {
	self.l.Lock()
	defer self.l.Unlock()

	vOffset, err := self.set(key, value, time.Now().Unix())
	fmt.Println(vOffset)
	return err
}

func (self *Bitcask) set(key, value string, tstamp int64) (int64, error) {
	//check the file size
	cur := self.acFile.offset
	fmt.Printf("before set key: %s, cur: %d\n",key,cur)
	vsz := uint32(len(value))
	ksz := uint32(len(key))

	//fix
	afterCur := cur + RECORD_HEADER_SIZE + int64(ksz+vsz)
	fmt.Printf("after set key: %s, cur: %d\n",key,afterCur)

	if afterCur > int64(self.maxFileSize) {
		//fix
		//new activity file
		err := self.acFile.Close()
		if err != nil {
			return -1, err
		}

		nfid := self.acFile.id + 1
		//nPath := self.data_path + "data" + strconv.Itoa(int(nfid))
		nPath := self.data_path + strconv.Itoa(int(nfid)) + ".data"

		nfp, err := os.OpenFile(nPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)

		if err != nil {
			return -1, err
		}

		nAcfile, err := NewBfile(nfp, nfid)

		if err != nil {
			return -1, err
		}

		self.acFile = nAcfile
	}

	//set in disk

	//voffset, err := self.acFile.WriteRecord(key, value, tstamp)
	//test

	voffset, err := self.acFile.WriteRecord([]byte(key), []byte(value), tstamp)

	if err != nil {
		return -1, err
	}

	i := &Indexer{
		fId:     self.acFile.id,
		tstamp:  tstamp,
		vsz:     vsz,
		voffset: voffset,
	}

	//fix

	//set in index map in memory
	self.kv.Set(key, i)

	return voffset, nil
}

func (self *Bitcask) Del(key string) error {
	tstamp := time.Now().Unix()

	//_, err := self.acFile.WriteRecord(key, "", tstamp)
	//test
	empty := []byte{0}
	_, err := self.acFile.WriteRecord([]byte(key), empty, tstamp)

	if err != nil {
		return err
	}

	// del in kv
	fmt.Printf("kv del %s\n", key)
	delErr := self.kv.Del(key)
	if delErr != nil {
		return delErr
	}

	return nil
}

func (self *Bitcask) Next() (*Record, error) {
	r, err := self.acFile.Next()

	if r == nil {
		if err != nil {
			fmt.Println(err)
		}
	}

	if err != nil && err == io.EOF {
		nfp, err := os.OpenFile(self.data_path+strconv.Itoa(int(self.acFile.id))+".data", os.O_RDONLY, 0766)
		if err != nil {
			return nil, err
		}

		nAcFile, _ := NewBfile(nfp, self.acFile.id+1)

		self.acFile.Close()
		self.acFile = nAcFile
	}

	return r, nil
}

//const HINTRECORD_HEADER_SIZE = 24
const HINTRECORD_HEADER_SIZE = 28

type hintRecord struct {
	dataFId uint32 // 4

	tstamp  int64  // 8
	ksz     uint32 // 4
	vsz     uint32 // 4
	voffset int64  //8
	key     string
}

func (self *Bitcask) WriteHint(hR *hintRecord) (int64, error) {
	cur := self.hintFile.offset

	afterCur := cur + HINTRECORD_HEADER_SIZE + int64(hR.ksz)

	if afterCur > int64(self.maxFileSize) {
		//new hint file
		err := self.hintFile.Close()

		if err != nil {
			return -1, err
		}

		nFid := self.hintFile.id + 1
		nPath := self.hint_path + strconv.Itoa(int(nFid)) + ".hint"

		nfp, err := os.OpenFile(nPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)

		if err != nil {
			return -1, err
		}

		nHintFile, err := NewBfile(nfp, nFid)
		if err != nil {
			return -1, err
		}

		self.hintFile = nHintFile
	}

	//fix
	offset, err := self.hintFile.WriteHintRecord(hR)

	return offset, err
}

func (self *hintRecord) transfer() ([]byte, error) {
	buf := new(bytes.Buffer)

	//test
	binary.Write(buf, binary.BigEndian, self.dataFId)

	binary.Write(buf, binary.BigEndian, self.tstamp)
	//fmt.Printf("tstamp: %d\n", len(buf.Bytes()))
	binary.Write(buf, binary.BigEndian, self.ksz)
	//fmt.Printf("ksz: %d\n", len(buf.Bytes()))
	binary.Write(buf, binary.BigEndian, self.vsz)
	//fmt.Printf("vsz: %d\n", len(buf.Bytes()))
	binary.Write(buf, binary.BigEndian, self.voffset)
	buf.Write([]byte(self.key))
	//fmt.Printf("key: %d\n", len(buf.Bytes()))
	//fmt.Println([]byte(key))

	return buf.Bytes(), nil
}

func (self *Bitcask) ParseHint(filePaths []string) error {
	for _, filePath := range filePaths {
		fp, err := os.OpenFile(self.hint_path+filePath, os.O_RDONLY, 0766)
		if err != nil {
			return err
		}

		//fix
		//FileID

		//b := strings.LastIndex(fp.Name(), "/") + 1
		//e := strings.LastIndex(fp.Name(), ".hint")
		//fileID, _ := strconv.Atoi(string(fp.Name()[b:e]))

		for {
			header := make([]byte, HINTRECORD_HEADER_SIZE)
			sz, err := fp.Read(header)

			if err != nil && err != io.EOF {
				panic(err)
			}

			if err == io.EOF {
				break
			}

			if sz != HINTRECORD_HEADER_SIZE {
				fmt.Printf("sz err")
				return fmt.Errorf("Read Header: exptectd %d, got %d", HINTRECORD_HEADER_SIZE, sz)
			}

			//build indexer
			i := new(Indexer)

			reader := bytes.NewReader(header)
			var ksz uint32

			//test
			var dataFid uint32
			binary.Read(reader, binary.BigEndian, &dataFid)

			binary.Read(reader, binary.BigEndian, &i.tstamp)
			binary.Read(reader, binary.BigEndian, &ksz)
			binary.Read(reader, binary.BigEndian, &i.vsz)
			binary.Read(reader, binary.BigEndian, &i.voffset)

			//fix
			//check ksz vsz

			//fix fid
			key := make([]byte, ksz)

			rksz, err := fp.Read(key)

			if err != nil && err != io.EOF {
				return err
			}

			if err == io.EOF {
				break
			}

			if rksz != int(ksz) {
				return fmt.Errorf("Read ksz: exptectd %d, got %d", ksz, sz)
			}

			i.fId = uint32(dataFid)

			//from hintfile to build hashmap
			self.kv.Set(string(key), i)
		}
	}
	return nil
}

func (self *Bitcask) GetFiles(path, fileType string) ([]string, error) {
	dirfp, err := os.OpenFile(path, os.O_RDONLY, 0766)
	if err != nil {
		return nil, err
	}

	defer dirfp.Close()

	flieNamelist, err := dirfp.Readdirnames(-1)

	if err != nil {
		return nil, err
	}

	var fileList []string

	for _, v := range flieNamelist {
		if strings.Contains(v, fileType) {
			fileList = append(fileList, v)
		}
	}

	return fileList, nil
}

func (self *Bitcask) getHintFiles() ([]string, error) {
	dirfp, err := os.OpenFile(self.hint_path, os.O_RDONLY, 0766)
	if err != nil {
		return nil, err
	}

	defer dirfp.Close()

	flieNamelist, err := dirfp.Readdirnames(-1)

	if err != nil {
		return nil, err
	}

	var hintFileList []string

	for _, v := range flieNamelist {
		if strings.Contains(v, ".hint") {
			hintFileList = append(hintFileList, v)
		}
	}

	return hintFileList, nil
}

func (self *Bitcask) removeAllHintFile() {
	dirfp, err := os.OpenFile(self.hint_path, os.O_RDONLY, 0766)
	if err != nil {
		fmt.Println(err)
	}

	defer dirfp.Close()
	flieNamelist, err := dirfp.Readdirnames(-1)

	if err != nil {
		fmt.Println(err)
	}

	for _, v := range flieNamelist {
		//fmt.Println(v)
		//fmt.Println(self.hint_path + v)
		os.Remove(self.hint_path + v)
	}
}

func (self *Bitcask) Merge() {
	self.l.Lock()
	defer self.l.Unlock()

	//remove old hint file and new one
	s, _ := ioutil.ReadDir(self.hint_path)
	if len(s) != 0 {
		//remove old hint file
		fmt.Printf("num of hint file: %d\n", len(s))
		self.removeAllHintFile()
	}

	//get  new hintfile
	nPath := self.hint_path + "1" + ".hint"
	nfp, err := os.OpenFile(nPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	if err != nil {
		fmt.Println(err)
	}
	nHintFile, err := NewBfile(nfp, 1)
	if err != nil {
		fmt.Println(err)
	}

	self.hintFile = nHintFile

	//data file list
	//fileList, err := self.getFiles(self.data_path, ".data")
	fileList, err := self.GetFiles(self.data_path, ".data")

	sort.Strings(fileList)

	//remove the activity file
	for k, v := range fileList {
		fmt.Println(v)
		if v == strconv.Itoa(int(self.acFile.id))+".data" {
			fileList = append(fileList[:k], fileList[k+1:]...)
		}
	}

	//fix err handle
	if err != nil {
		fmt.Println(err)
	}

	//traversal all data file
	for _, fn := range fileList {
		fmt.Printf("merge fn: %s\n", fn)
		fmt.Println(self.acFile.id)

		if fn == strconv.Itoa(int(self.acFile.id))+".data" {
			fmt.Println(fn)
			continue
		}

		fp, err := os.OpenFile(self.data_path+fn, os.O_RDONLY, 0766)

		if err != nil {
			fmt.Println(err)
		}

		//get fId
		b := strings.LastIndex(fp.Name(), "/") + 1
		e := strings.LastIndex(fp.Name(), ".data")
		fId, _ := strconv.Atoi(string(fp.Name()[b:e]))
		//fmt.Printf("merge Fid: %d\n", fId)

		//test
		//filter the acFile
		if uint32(fId) == self.acFile.id {
			continue
		}

		bf, err := NewBfile(fp, uint32(fId))

		if err != nil {
			fmt.Println(err)
		}

		//scan data file
		for {
			r, err := bf.Next()

			//if r!= nil{
			//	fmt.Printf("merge r %V\n",r)
			//}
			//fmt.Println("here1")
			//fmt.Println(r.value)

			if err != nil && err != io.EOF {
				fmt.Println(err)
			}

			if err == io.EOF {
				break
			}

			// Record be deleted
			//empty := []byte{0}
			//test

			//if string(r.key) == "EE"{
			//	fmt.Printf("key: EE,value: %s\n",string(r.value))
			//}
			//
			//empty := []byte{0}
			//if r.value[0] == empty[0]{
			//	continue
			//}
			//
			//if string(r.key) == "EE"{
			//	fmt.Printf("after continue key: EE,value: %s\n",string(r.value))
			//}

			//fmt.Println("here4")
			//test
			//fmt.Println(r.key)
			i, err := self.kv.Get(string(r.key))

			//be deleted
			if i == nil {
				fmt.Printf("nil continue: %V\n", string(r.key))
				continue
			}

			//冗余数据
			if r.tstamp < i.tstamp {
				fmt.Printf("tstamp continue: %V\n", r)
				continue
			}

			//r older than indexer in memory
			//if i != nil && i.tstamp > r.tstamp {
			//	continue
			//}

			//fmt.Println("here2")
			//write in activity file
			//test
			offset, err := self.set(string(r.key), string(r.value), r.tstamp)

			if err != nil {
				fmt.Println(err)
			}

			//key not in kv || key is not latest
			if i != nil && i.tstamp < r.tstamp {
				//write in kv
				nIndex := &Indexer{
					fId:     self.acFile.id,
					tstamp:  r.tstamp,
					vsz:     r.vsz,
					voffset: offset,
				}

				//test
				self.kv.Set(string(r.key), nIndex)
			}

			//if i != nil && i.tstamp < r.tstamp{
			//	//write in kv
			//	nIndex := &Indexer{
			//		fId:self.acFile.id,
			//		tstamp:r.tstamp,
			//		vsz:r.vsz,
			//		voffset:offset,
			//	}
			//
			//	self.kv.Set(r.key,nIndex)
			//}

			//write hint file
			hR := &hintRecord{
				//test
				dataFId: self.acFile.id,

				tstamp:  r.tstamp,
				ksz:     r.ksz,
				vsz:     r.vsz,
				voffset: offset,
				//test
				key: string(r.key),
			}

			//fmt.Println("here3")
			_, hintErr := self.WriteHint(hR)

			if hintErr != nil {
				fmt.Println(hintErr)
			}
			fmt.Printf("merge WriteHint key: %s\n", hR.key)

		}
		//remove data file
		os.Remove(self.data_path + fn)
	}

	fmt.Printf("activity fId: %d\n", self.acFile.id)

}

func (self *Bitcask) Close() {
	self.l.Lock()
	defer self.l.Unlock()

	//remove old hint file and new one
	s, _ := ioutil.ReadDir(self.hint_path)
	if len(s) != 0 {
		//remove old hint file
		fmt.Printf("num of hint file: %d\n", len(s))
		self.removeAllHintFile()
	}

	//get  new hintfile
	nPath := self.hint_path + "1" + ".hint"
	nfp, err := os.OpenFile(nPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	if err != nil {
		fmt.Println(err)
	}
	nHintFile, err := NewBfile(nfp, 1)
	if err != nil {
		fmt.Println(err)
	}

	self.hintFile = nHintFile

	//data file list
	fileList, err := self.GetFiles(self.data_path, ".data")

	sort.Strings(fileList)

	//acfile := fileList[len(fileList) - 1]
	//acfp,err := os.OpenFile(self.data_path + acfile,os.O_RDONLY,0766)
	//acfi,err := acfp.Stat()
	//seek := acfi.Size()
	//acfp.Close()

	acFId := self.acFile.id
	acOffest := self.acFile.offset

	var cur int64

	//fix err handle
	if err != nil {
		fmt.Println(err)
	}

	//traversal all data file
	for _, fn := range fileList {
		cur = 0
		fmt.Printf("merge fn: %s\n", fn)
		fmt.Println(self.acFile.id)

		fp, err := os.OpenFile(self.data_path+fn, os.O_RDONLY, 0766)

		if err != nil {
			fmt.Println(err)
		}

		//get fId
		b := strings.LastIndex(fp.Name(), "/") + 1
		e := strings.LastIndex(fp.Name(), ".data")
		fId, _ := strconv.Atoi(string(fp.Name()[b:e]))
		fmt.Printf("merge Fid: %d\n", fId)

		bf, err := NewBfile(fp, uint32(fId))

		if err != nil {
			fmt.Println(err)
		}

		//bf.f.Seek(0,0)
		//scan data file
		for {
			r, err := bf.Next()

			if err != nil && err != io.EOF {
				fmt.Println(err)
			}

			if err == io.EOF {
				break
			}

			//结束
			if cur+RECORD_HEADER_SIZE+int64(r.ksz+r.vsz) > acOffest && uint32(fId) == acFId {
				break
			}

			cur += RECORD_HEADER_SIZE + int64(r.ksz+r.vsz)

			i, err := self.kv.Get(string(r.key))

			//be deleted
			if i == nil {
				fmt.Printf("CLOSE nil continue: %V\n", string(r.key))
				continue
			}

			//冗余数据
			if r.tstamp < i.tstamp {
				fmt.Printf("CLOSE tstamp continue: %V\n", r)
				continue
			}

			offset, err := self.set(string(r.key), string(r.value), r.tstamp)

			if err != nil {
				fmt.Println(err)
			}

			//key not in kv || key is not latest
			if i == nil || (i != nil && i.tstamp < r.tstamp) {
				//write in kv
				nIndex := &Indexer{
					fId:     self.acFile.id,
					tstamp:  r.tstamp,
					vsz:     r.vsz,
					voffset: offset,
				}

				//test
				self.kv.Set(string(r.key), nIndex)
			}

			//write hint file
			hR := &hintRecord{
				//test
				dataFId: self.acFile.id,

				tstamp:  r.tstamp,
				ksz:     r.ksz,
				vsz:     r.vsz,
				voffset: offset,
				//test
				key: string(r.key),
			}

			_, hintErr := self.WriteHint(hR)

			if hintErr != nil {
				fmt.Println(hintErr)
			}

			fmt.Printf("CLOSE merge WriteHint key: %s\n", hR.key)

		}
		//remove data file
		if uint32(fId) != acFId {
			os.Remove(self.data_path + fn)
		}
	}
}

func (self *Bitcask) load() error {
	fileList, err := self.GetFiles("./hint", ".hint")

	if err != nil {
		return err
	}

	parseErr := self.ParseHint(fileList)

	if parseErr != nil {
		return parseErr
	}

	return nil
}

func (self *Bitcask) Scan() error {
	fileList, err := self.GetFiles(self.data_path, ".data")
	if err != nil {
		return err
	}
	sort.Strings(fileList)

	//traversal all data file
	for _, fn := range fileList {
		fp, err := os.OpenFile(self.data_path+fn, os.O_RDONLY, 0766)

		if err != nil {
			return err
		}

		//get fId
		b := strings.LastIndex(fp.Name(), "/") + 1
		e := strings.LastIndex(fp.Name(), ".data")
		fId, _ := strconv.Atoi(string(fp.Name()[b:e]))

		bf, err := NewBfile(fp, uint32(fId))
		if err != nil {
			return err
		}
		//scan data file
		var (
			cur int64
		)

		for {
			r, err := bf.Next()

			if err != nil && err != io.EOF {
				return err
			}

			if err == io.EOF {
				break
			}

			cur = cur + RECORD_HEADER_SIZE + int64(r.ksz)

			i, err := self.kv.Get(string(r.key))
			emtpy := []byte{0}
			if r.value[0] == emtpy[0]  && r.vsz == 1{
				self.kv.Del(string(r.key))
				continue
			}

			//be deleted
			if i == nil || i.tstamp < r.tstamp {
				nIndex := &Indexer{
					fId:     uint32(fId),
					tstamp:  r.tstamp,
					vsz:     r.vsz,
					voffset: cur,
				}

				self.kv.Set(string(r.key), nIndex)

				cur += 1
			}
		}
	}
	//remove data file
	return nil
}
