package memtable

import (
	"bytes"
	"sort"
)

type MemTable interface {
	Put(key, value []byte)
	Get(key []byte) ([]byte, bool)
	Delete(key []byte)
	Show()
	Iterator() Iterator
}

type BTreeMemTable struct {
	btree *btree
}

func (b *BTreeMemTable) NewBTreeMemTableIter() Iterator {
	ans := []*data{}
	b.btree.InOrderTraversal(func(d *data) {
		ans = append(ans, d)
	})
	return &BTreeMemTableIter{
		list:     ans,
		length:   len(ans),
		curIndex: 0,
	}
}

func (b *BTreeMemTable) Iterator() Iterator {
	ans := []*data{}
	b.btree.InOrderTraversal(func(d *data) {
		if !d.deleted {
			ans = append(ans, d)
		}
	})
	return &BTreeMemTableIter{
		list:     ans,
		curIndex: 0,
		length:   len(ans),
	}
}

func (b *BTreeMemTable) Put(key, value []byte) {
	b.btree.Put(newData(key, value))
}

func (b *BTreeMemTable) Get(key []byte) ([]byte, bool) {
	ans := newData(key, nil)
	ok := b.btree.Get(ans)
	return ans.value, ok
}

func (b *BTreeMemTable) Delete(key []byte) {
	b.btree.Remove(newData(key, nil))
}
func (b *BTreeMemTable) Show() {
	b.btree.PrintTree()
}
func NewBTreeMemTable() MemTable {
	return &BTreeMemTable{btree: NewBTree(9)}
}

type Iterator interface {
	Prev()
	Next()
	Valid() bool
	Seek([]byte)
	Curr() ([]byte, []byte)
}
type BTreeMemTableIter struct {
	list     []*data
	curIndex int
	length   int
}

func (i *BTreeMemTableIter) Prev() {
	i.curIndex--
}

func (i *BTreeMemTableIter) Next() {
	i.curIndex++
}
func (i *BTreeMemTableIter) Valid() bool {
	return i.curIndex >= 0 && i.curIndex < i.length
}
func (i *BTreeMemTableIter) Seek(key []byte) {
	i.curIndex = sort.Search(len(i.list), func(j int) bool {
		return bytes.Compare(i.list[j].key, key) >= 0
	})
}
func (i *BTreeMemTableIter) Curr() ([]byte, []byte) {
	return i.list[i.curIndex].key, i.list[i.curIndex].value
}
