package main

import (
	"bitcask/src"
	"fmt"
)

func main() {
	//f, _ := os.OpenFile(testFileNAME, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	//
	//bf, _ := NewBfile(f, 1)
	//
	//hashmap := NewHashMap()
	//
	//key1 := "A"
	//value1 := "a"
	//
	//tstamp := time.Now().Unix()
	//voffset, _ := bf.WriteRecord(key1, value1, tstamp)
	//
	//fmt.Println(voffset)
	//
	//index1 := &Indexer{
	//	fId:     1,
	//	tstamp:  tstamp,
	//	vsz:     uint32(len(value1)),
	//	voffset: voffset,
	//}
	//
	//hashmap.Set(key1, index1)
	//
	//key2 := "B"
	//value2 := "b"
	//
	//tstamp2 := time.Now().Unix()
	//voffset2, _ := bf.WriteRecord(key2, value2, tstamp)
	//
	//fmt.Println(voffset2)
	//
	//index2 := &Indexer{
	//	fId:     1,
	//	tstamp:  tstamp2,
	//	vsz:     uint32(len(value2)),
	//	voffset: voffset2,
	//}
	//
	//
	//hashmap.Set(key2, index2)
	//
	//v1,_ := hashmap.Get(key2)
	//v,_:= bf.ReadRecord(v1)
	//fmt.Println(v)

	//test bitcask

	b := bitcask.NewBitcask()
	//b.Scan()
	//fileList, err := b.GetFiles("./hint", ".hint")
	//
	//if err != nil {
	//	fmt.Println(err)
	//}
	//
	//b.ParseHint(fileList)
	b.Scan()

	//println("hint show")
	v1, _ := b.Get("AA")
	fmt.Printf("key: AA,val: %s\n", v1)
	v2, _ := b.Get("BB")
	fmt.Printf("key: BB,val: %s\n", v2)
	v3, _ := b.Get("CC")
	fmt.Printf("key: CC,val: %s\n", v3)
	v4, _ := b.Get("DD")
	fmt.Printf("key: DD,val: %s\n", v4)
	v5, _ := b.Get("EE")
	fmt.Printf("key: EE,val: %s\n", v5)
	v6, _ := b.Get("FF")
	fmt.Printf("key: FF,val: %s\n", v6)
	v7, _ := b.Get("GG")
	fmt.Printf("key: GG,val: %s\n", v7)
	v8, _ := b.Get("HH")
	fmt.Printf("key: HH,val: %s\n", v8)

	fmt.Println()
	//
	//b.Set("AA", "a")
	//b.Set("BB", "b")
	//b.Set("CC", "c")
	//b.Set("DD", "d")
	//b.Set("EE", "e")
	//b.Set("FF", "f")
	//b.Set("GG", "g")
	//b.Set("HH", "h")
	//
	//b.Del("EE")

	//defer b.Close()
	//var wg sync.WaitGroup
	//
	//wg.Add(2)
	//go func() {
	//	defer wg.Done()
	//	for {
	//		time.Sleep(8 * time.Second)
	//		b.Merge()
	//	}
	//}()
	//
	//go func() {
	//	defer wg.Done()
	//	for {
	//		println("default show")
	//
	//		v1, _ := b.Get("AA")
	//		fmt.Printf("key: AA,val: %s\n", v1)
	//		v2, _ := b.Get("BB")
	//		fmt.Printf("key: BB,val: %s\n", v2)
	//		v3, _ := b.Get("CC")
	//		fmt.Printf("key: CC,val: %s\n", v3)
	//		v4, _ := b.Get("DD")
	//		fmt.Printf("key: DD,val: %s\n", v4)
	//		v5, _ := b.Get("EE")
	//		fmt.Printf("key: EE,val: %s\n", v5)
	//		v6, _ := b.Get("FF")
	//		fmt.Printf("key: FF,val: %s\n", v6)
	//		v7, _ := b.Get("GG")
	//		fmt.Printf("key: GG,val: %s\n", v7)
	//		v8, _ := b.Get("HH")
	//		fmt.Printf("key: HH,val: %s\n", v8)
	//
	//		fmt.Println()
	//
	//		time.Sleep(3 * time.Second)
	//	}
	//}()
	//
	//time.Sleep(25 * time.Second)
	//panic(1)
	//
	//wg.Wait()

	//fmt.Println("hello")

	//test bitcask Del
	//b.Del("AA")
	//
	//v3,err := b.Get("AA")
	//
	//if err != nil {
	//	fmt.Println("not found")
	//}
	//
	//fmt.Println(v3)
	//
	//b.Set("CC","c")

	//b.acfile.f.Seek(0,0)

	//for {
	//	r,err := b.Next()
	//
	//
	//	if err == io.EOF {
	//		fmt.Println("eof")
	//		break
	//	} else if err != nil{
	//		fmt.Println(err)
	//		break
	//	}
	//
	//
	//	fmt.Println("value")
	//	fmt.Printf("%V\n",r)
	//}

	//var wg sync.WaitGroup
	//wg.Add(2)
	//defer func() {
	//	fmt.Printf("7777777777")
	//}()
	//
	//go func() {
	//	//defer wg.Done()
	//	for {
	//		time.Sleep(2 * time.Second)
	//	}
	//}()
	//
	//go func() {
	//	//defer wg.Done()
	//	for {
	//		time.Sleep(3 * time.Second)
	//	}
	//}()
	//
	//
	//for {
	//	time.Sleep(5 * time.Second)
	//	panic(-2)
	//}
	//wg.Wait()
	//var wg sync.WaitGroup
	//wg.Add(1)
	//go func() {
	//	defer wg.Done()
	//	c := make(chan os.Signal, 1)
	//	signal.Notify(c, os.Interrupt, os.Kill)
	//
	//	s := <-c
	//	fmt.Println(s)
	//	//b.Close()
	//}()
	//
	//wg.Wait()

}
