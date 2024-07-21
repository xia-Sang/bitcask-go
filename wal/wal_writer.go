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
	keySize := len(key)
	valueSize := len(value)
	totalSize := WalBufferSize + keySize + valueSize

	buf := make([]byte, totalSize)
	index := crc32.Size

	// 存储键值对大小
	index += binary.PutUvarint(buf[index:], uint64(keySize))
	index += binary.PutUvarint(buf[index:], uint64(valueSize))

	// 存储键和值
	copy(buf[index:], key)
	copy(buf[index+keySize:], value)

	// 计算并存储 CRC 校验码
	crc := crc32.ChecksumIEEE(buf[crc32.Size : index+keySize+valueSize])

	binary.BigEndian.PutUint32(buf[:crc32.Size], crc)
	// 写入缓冲区到写入器
	if _, err := w.Write(buf[:index+keySize+valueSize]); err != nil {
		return err
	}

	return nil
}
