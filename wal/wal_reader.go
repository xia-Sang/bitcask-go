package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/xia-Sang/bitcask/memtable"
)

// 这个里面是不断地进行追加写操作
type WalReader struct {
	filePath string
	wal      *os.File
}

func NewWalReader(filePath string) (*WalReader, error) {
	fp, err := os.OpenFile(filePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &WalReader{filePath: filePath, wal: fp}, nil
}
func (w *WalReader) RestoreToMemTable(table memtable.MemTable) error {
	defer func() {
		_, _ = w.wal.Seek(0, io.SeekStart)
	}()

	err := w.read()
	if err != nil {
		return err
	}

	return nil
}
func (w *WalReader) Close() error {
	return w.wal.Close()
}
func (w *WalReader) read() error {
	w.wal.Seek(0, io.SeekStart)
	offset := 0
	for {
		key, value, length, err := w.reader(int64(offset))
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		offset += length
		fmt.Printf("%s:%s\n", key, value)
	}
	return nil
}

func (w *WalReader) readAt(b []byte, off int64) (n int, err error) {
	return w.wal.ReadAt(b, off)
}
func (w *WalReader) reader(off int64) ([]byte, []byte, int, error) {
	buf := make([]byte, WalBufferSize)
	if _, err := w.readAt(buf, off); err != nil {
		return nil, nil, 0, err
	}
	index := crc32.Size
	binary.BigEndian.Uint32(buf[:index])
	keySize, n := binary.Uvarint(buf[index:])
	index += n
	valueSize, n := binary.Uvarint(buf[index:])
	index += n
	kvBuf := make([]byte, keySize+valueSize)

	if _, err := w.readAt(kvBuf, off+int64(index)); err != nil {
		return nil, nil, 0, err
	}
	return kvBuf[:keySize], kvBuf[keySize:], index + int(keySize+valueSize), nil
}

// func reader(r io.Reader) ([]byte, []byte, error) {
// 	buf := make([]byte, WalBufferSize)
// 	readCnt, err := r.Read(buf)
// 	if err != nil {
// 		return nil, nil, err
// 	}
// 	if readCnt == 0 {
// 		return nil, nil, nil
// 	}
// 	index := crc32.Size
// 	binary.BigEndian.Uint32(buf[:index])
// 	keySize, n := binary.Uvarint(buf[index:])
// 	index += n
// 	valueSize, n := binary.Uvarint(buf[index:])
// 	index += n

// 	start := WalBufferSize - index
// 	remainKvSize := keySize + valueSize - uint64(start)
// 	kvBuf := make([]byte, remainKvSize)

// 	readCnt, err = r.Read(kvBuf)
// 	if err != nil {
// 		return nil, nil, err
// 	}
// 	if readCnt == 0 {
// 		return nil, nil, nil
// 	}
// 	kvBuf = append(buf[index:], kvBuf...)
// 	return kvBuf[:keySize], kvBuf[keySize:], nil
// }
