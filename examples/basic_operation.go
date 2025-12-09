package main

// TODO: 第一次见到这种用法，这是什么样子的呢？
import (
	bitcask "bitcask-go"
	"fmt"
)

func main() {
	setup := bitcask.DefaultSetup
	setup.DirPath = "/tmp/bitcask-go"
	db, err := bitcask.Open(setup)
	if err != nil {
		panic(err) // TODO: 同 log.Fatal(err) 有什么区别呢？
	}

	// 添加键值对 —— ("name", "bitcask")
	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}

	// 获取对应键的值 —— 应该返回 "bitcask"
	value, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val = ", string(value)) // Pass！

	err = db.Delete([]byte("name"))
	if err != nil {
		panic(err)
	}

}
