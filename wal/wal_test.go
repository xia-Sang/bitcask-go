package wal

import (
	"fmt"
	"testing"

	"github.com/xia-Sang/bitcask/memtable"
	"github.com/xia-Sang/bitcask/utils"
)

func TestWalWriter(t *testing.T) {
	key := []byte("xiasang")
	writer, err := TestWal()
	t.Log(err)
	_, err = writer.Write(key, key)
	t.Log(err)
}
func TestWalWriters(t *testing.T) {
	writer, err := TestWal()
	t.Log(err)
	for i := range 120 {
		key, value := utils.GenerateKey(i), utils.GenerateRandomBytes(12)
		fmt.Printf("k:%s,v:%s\n", key, value)
		_, err = writer.Write(key, value)
		t.Log(err)
		if i < 100 {
			_, err = writer.Write(key, nil)
			t.Log(err)
		}
	}

}
func TestWalReader(t *testing.T) {
	// key := []byte("xiasang")
	reader, err := TestWal()
	t.Log(err)
	btree := memtable.NewBTreeMemTable()
	err = reader.Read(btree)
	t.Log(err)
	for iter := btree.Iterator(); iter.Valid(); iter.Next() {
		key, value := iter.Curr()
		fmt.Printf("%s:%v\n", key, value)
	}

}
