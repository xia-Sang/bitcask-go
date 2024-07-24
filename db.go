package bitcask

import (
	"bytes"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/xia-Sang/bitcask/memtable"
	"github.com/xia-Sang/bitcask/wal"
)

type Db struct {
	opts        *Options //配置选项
	activeFiles *wal.Wal
	olderFiles  map[int]*wal.Wal
	memTable    memtable.MemTable
	mu          *sync.RWMutex
}
type Data struct {
	Key   []byte
	Value []byte
}

func (db *Db) ListKeys() (ans []*Data) {
	for iter := db.memTable.Iterator(); iter.Valid(); iter.Next() {
		key, pos := iter.Curr()
		val := db.getValueByPos(pos.(*wal.Pos))
		if !bytes.Equal(val, []byte("")) {
			ans = append(ans, &Data{Key: key, Value: val})
		}
	}
	return
}
func (db *Db) Fold(fn func(key, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	for iter := db.memTable.Iterator(); iter.Valid(); iter.Next() {
		key, pos := iter.Curr()
		val := db.getValueByPos(pos.(*wal.Pos))
		if !bytes.Equal(val, []byte("")) {
			if !fn(key, val) {
				break
			}
		}
	}
	return nil
}
func (db *Db) newActiveFile() error {
	var fileId int
	if db.activeFiles != nil {
		fileId = db.activeFiles.FileId
		if err := db.activeFiles.Sync(); err != nil {
			return err
		}
		db.olderFiles[fileId] = db.activeFiles
	} else {
		fileId = 0
	}

	newActive, err := wal.NewWal(db.opts.DirPath, fileId+1)
	if err != nil {
		return err
	}
	db.activeFiles = newActive
	return nil
}
func NewDb(opts *Options) *Db {
	db := &Db{
		opts:       opts,
		memTable:   memtable.NewBTreeMemTable(),
		mu:         &sync.RWMutex{},
		olderFiles: map[int]*wal.Wal{},
	}
	if err := db.constructMemTable(); err != nil {
		panic(err)
	}
	return db
}
func (db *Db) Open(filename string) error {
	return nil
}
func (db *Db) Close() {
	db.opts = nil
	db.activeFiles = nil
	db.memTable.Clear()
	db.mu = nil
	db.olderFiles = nil
}
func (db *Db) Get(key []byte) ([]byte, bool) {
	pos, ok := db.memTable.Get(key)
	if !ok {
		return nil, false
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	val := db.getValueByPos(pos.(*wal.Pos))
	return val, ok && !bytes.Equal(val, []byte(""))
}
func (db *Db) getValueByPos(pos *wal.Pos) []byte {
	var w *wal.Wal
	if db.activeFiles.FileId == pos.FileId {
		w = db.activeFiles
	} else {
		w = db.olderFiles[pos.FileId]
	}
	if w == nil {
		return nil
	}
	_, val, err := w.ReadBuf(pos.Offset, pos.Length)
	if err != nil {
		return nil
	}
	return val
}
func (db *Db) checkOverFlow() bool {
	return db.opts.MaxFileSize <= db.activeFiles.Size()
}
func (db *Db) Put(key []byte, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 如果数据溢出 开辟新的
	if db.checkOverFlow() {
		if err := db.newActiveFile(); err != nil {
			return err
		}
	}

	pos, err := db.activeFiles.Write(key, value)
	if err != nil {
		return err
	}
	db.memTable.Put(key, pos) //存储进入内存
	return nil
}
func (db *Db) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	_, err := db.activeFiles.Write(key, nil)
	if err != nil {
		return err
	}
	db.memTable.Delete(key)
	return nil
}
func (db *Db) constructMemTable() error {
	entries, err := os.ReadDir(db.opts.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}
		fileIds = append(fileIds, walFileMemTableIndex(entry.Name()))
	}
	if len(fileIds) == 0 {
		return db.newActiveFile()
	}
	return db.restoreMemTable(fileIds)
}
func walFileMemTableIndex(walFile string) int {
	rawIndex := strings.TrimSuffix(walFile, wal.WalFileName)
	index, err := strconv.Atoi(rawIndex)
	if err != nil {
		panic(err)
	}
	return index
}

func (db *Db) restoreMemTable(fileIds []int) error {
	sort.Slice(fileIds, func(i, j int) bool {
		return fileIds[i] <= fileIds[j]
	})

	for idx, fileId := range fileIds {
		walReader, err := wal.NewWal(db.opts.DirPath, fileId)
		if err != nil {
			return err
		}
		memTable := db.memTable
		if err = walReader.Read(memTable); err != nil {
			return err
		}
		if idx == len(fileIds)-1 {
			db.activeFiles = walReader
		} else {
			db.olderFiles[fileId] = walReader
		}
	}
	return nil
}

func (db *Db) CloseAndMerge() error {
	lastFileId := db.activeFiles.FileId
	if err := db.newActiveFile(); err != nil {
		return err
	}
	for iter := db.memTable.Iterator(); iter.Valid(); iter.Next() {
		key, pos := iter.Curr()
		val := db.getValueByPos(pos.(*wal.Pos))
		if !bytes.Equal(val, []byte("")) {
			if db.checkOverFlow() {
				if err := db.newActiveFile(); err != nil {
					return err
				}
			}
			if _, err := db.activeFiles.Write(key, val); err != nil {
				return err
			}
		}
	}
	for _, oldFile := range db.olderFiles {
		if oldFile.FileId <= lastFileId {
			if err := oldFile.CloseAndDelete(); err != nil {
				return err
			}
		}
	}
	db.Close()
	return nil
}
