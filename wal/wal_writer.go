package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

const WalBufferSize = 4 + 2*binary.MaxVarintLen64

// 这个里面是不断地进行追加写操作
type WalWriter struct {
	filePath string
	wal      *os.File
}

func NewWalWriter(filePath string) (*WalWriter, error) {
	fp, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &WalWriter{filePath: filePath, wal: fp}, nil
}
func (w *WalWriter) Write(key, value []byte) error {
	return writer(w.wal, key, value)
}
func writer(w io.Writer, key, value []byte) error {
	index := crc32.Size
	// 实现变长存储
	keySize := len(key)
	valueSize := len(value)
	buf := make([]byte, WalBufferSize+keySize+valueSize)
	binary.PutUvarint(buf[index:], uint64(keySize))
	index += 8
	binary.PutUvarint(buf[index:], uint64(valueSize))
	index += 8
	copy(buf[index:], key)
	copy(buf[index+keySize:], value)

	crc := crc32.ChecksumIEEE(buf[crc32.Size : index+keySize+valueSize])
	binary.BigEndian.PutUint32(buf[:crc32.Size], crc)
	if _, err := w.Write(buf); err != nil {
		return err
	}
	return nil
}
