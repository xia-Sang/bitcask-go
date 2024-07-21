package wal

import (
	"fmt"
	"testing"

	"github.com/xia-Sang/bitcask/utils"
)

func TestWalWriter(t *testing.T) {
	key := []byte("xiasang")
	writer, err := NewWalWriter("wal.db")
	t.Log(err)
	err = writer.Write(key, key)
	t.Log(err)
}
func TestWalWriters(t *testing.T) {
	writer, err := NewWalWriter("wal.db")
	t.Log(err)
	for i := range 12 {
		key, value := utils.GenerateKey(i), utils.GenerateRandomBytes(12)
		fmt.Printf("k:%s,v:%s\n", key, value)
		err = writer.Write(key, value)
		t.Log(err)
	}
}
func TestWalReader(t *testing.T) {
	// key := []byte("xiasang")
	reader, err := NewWalReader("wal.db")
	t.Log(err)
	err = reader.read()
	t.Log(err)
}
