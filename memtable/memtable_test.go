package memtable

import (
	"fmt"
	"testing"

	"github.com/xia-Sang/bitcask/utils"
)

// 随机写入数据
func TestMemTable(t *testing.T) {
	memTable := NewBTreeMemTable()
	for _, i := range utils.RandomIntsInRange(12, 0, 100) {
		key, value := utils.GenerateKey(i), utils.GenerateRandomBytes(12)
		memTable.Put(key, value)
	}

	memTable.Show()
	memTable.Iterator()
}

// 多次写入数据
func TestMemTableWriter(t *testing.T) {
	memTable := NewBTreeMemTable()

	key, value := []byte("a"), utils.GenerateRandomBytes(12)
	memTable.Put(key, value)
	memTable.Put(key, value)
	memTable.Put(key, value)
	memTable.Put(key, value)
	memTable.Put(key, value)
	memTable.Show()
	memTable.Iterator()
}

// 检查迭代器功能
func TestMemTableWriter1(t *testing.T) {
	memTable := NewBTreeMemTable()

	key, value := []byte("a"), utils.GenerateRandomBytes(12)
	memTable.Put(key, value)
	memTable.Put([]byte("b"), value)
	memTable.Put([]byte("c"), value)
	memTable.Put([]byte("d"), value)
	memTable.Put([]byte("e"), value)
	memTable.Show()
	memTable.Iterator()
	bt := memTable.Iterator()
	for bt.Valid() {
		key, val := bt.Curr()
		fmt.Printf("%s:%s\n", key, val)
		bt.Next()
	}
	fmt.Println()
	bt.Seek([]byte("a"))
	for bt.Valid() {
		key, val := bt.Curr()
		fmt.Printf("%s:%s\n", key, val)
		bt.Next()
	}
	fmt.Println()

	bt.Seek([]byte("f"))
	for bt.Valid() {
		key, val := bt.Curr()
		fmt.Printf("%s:%s\n", key, val)
		bt.Next()
	}
	fmt.Println()
}

// 测试删除功能
func TestMemTableDelete(t *testing.T) {
	memTable := NewBTreeMemTable()
	ls := utils.RandomIntsInRange(12, 0, 100)
	for _, i := range ls {
		key, value := utils.GenerateKey(i), utils.GenerateRandomBytes(12)
		memTable.Put(key, value)
	}

	for bt := memTable.Iterator(); bt.Valid(); bt.Next() {
		key, val := bt.Curr()
		fmt.Printf("%s:%s\n", key, val)

	}
	fmt.Println()
	deletedKey := ls[2]
	memTable.Delete(utils.GenerateKey(deletedKey))
	for bt := memTable.Iterator(); bt.Valid(); bt.Next() {
		key, val := bt.Curr()
		fmt.Printf("%s:%s\n", key, val)

	}
	t.Log(deletedKey)
}
