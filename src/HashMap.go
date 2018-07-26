package bitcask

import (
	"fmt"
	"sync"
)

//映射表中的value  20
type Indexer struct {
	fId     uint32 //4
	tstamp  int64  //8
	vsz     uint32 //4
	voffset int64  //8
}
type HashMap struct {
	m map[string]*Indexer
	l sync.RWMutex
}

func NewHashMap() *HashMap {
	ret := new(HashMap)

	ret.m = make(map[string]*Indexer)

	return ret
}

func (self *HashMap) Get(key string) (*Indexer, error) {
	self.l.RLock()
	defer self.l.RUnlock()

	if v, ok := self.m[key]; ok {
		//fmt.Println(v)
		return v, nil
	}

	return nil, fmt.Errorf("key not found")
}

func (self *HashMap) Set(key string, value *Indexer) error {
	self.l.Lock()
	defer self.l.Unlock()

	self.m[key] = value

	//fmt.Println(self.m[key])
	return nil
}

func (self *HashMap) Del(key string) error {
	self.l.Lock()
	defer self.l.Unlock()

	delete(self.m, key)

	return nil
}
