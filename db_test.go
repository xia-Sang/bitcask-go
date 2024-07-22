package bitcask

import (
	"fmt"
	"testing"

	"github.com/xia-Sang/bitcask/utils"
)

func TestNew(t *testing.T) {
	db := NewDb(NewOptions("./data"))
	for i := range 12 {
		key, value := utils.GenerateKey(i), utils.GenerateRandomBytes(12)
		fmt.Printf("k:%s,v:%s\n", key, value)
		db.Put(key, value)
		//if i < 10 {
		//	err := db.Delete(key)
		//	t.Log(err)
		//}
	}
	for i := range 12 {
		key := utils.GenerateKey(i)
		val, ok := db.Get(key)
		t.Log(string(key), string(val), ok)
	}
}
func TestNew1(t *testing.T) {
	db := NewDb(NewOptions("./data"))
	for i := range 120 {
		key, value := utils.GenerateKey(i), utils.GenerateRandomBytes(12)
		fmt.Printf("k:%s,v:%s\n", key, value)
		err := db.Put(key, value)
		t.Log(err)
		if i < 100 {
			err := db.Delete(key)
			t.Log(err)
		}
	}
	for iter := db.memTable.Iterator(); iter.Valid(); iter.Next() {
		key, val := iter.Curr()
		fmt.Printf("%s: %v\n", string(key), val)
	}
	for i := range 12 {
		key := utils.GenerateKey(i)
		val, ok := db.Get(key)
		t.Log(string(key), string(val), ok)
	}
}
func TestNew2(t *testing.T) {
	db := NewDb(NewOptions("./data"))

	for iter := db.memTable.Iterator(); iter.Valid(); iter.Next() {
		key, val := iter.Curr()
		fmt.Printf("%s: %v\n", string(key), val)
	}
	for i := range 120 {
		key := utils.GenerateKey(i)
		val, ok := db.Get(key)
		t.Log(string(key), string(val), ok)
	}
}
func TestNew3(t *testing.T) {
	db := NewDb(NewOptions("./data"))

	for iter := db.memTable.Iterator(); iter.Valid(); iter.Next() {
		key, val := iter.Curr()
		fmt.Printf("%s: %v\n", string(key), val)
	}
	for i := range 120 {
		key := utils.GenerateKey(i)
		val, ok := db.Get(key)
		t.Log(string(key), string(val), ok)
	}
	err := db.CloseAndMerge()
	t.Log(err)
}
