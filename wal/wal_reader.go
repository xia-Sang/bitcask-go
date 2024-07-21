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

func (w *WalReader) Close() error {
	return w.wal.Close()
}
func (w *WalReader) read(table memtable.MemTable) error {
	offset := 0
	for {
		key, value, length, err := w.reader(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if value != nil {
			table.Put(key, value)
		} else {
			table.Delete(key)
		}
		offset += length
	}
	return nil
}
func readData(r io.ReaderAt, offset int, size int) ([]byte, []byte, int, error) {
	var headerSize = WalBufferSize
	if size < WalBufferSize+offset {
		headerSize = size - offset //最后的剩余容量
	}
	buf := make([]byte, headerSize)
	cnt, err := r.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, nil, 0, err
	}
	if cnt == 0 {
		return nil, nil, 0, io.EOF
	}
	index := crc32.Size
	// 读取并忽略 CRC 校验码
	expectCrc32 := binary.BigEndian.Uint32(buf[:index])
	// 解码键的大小
	keySize, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return nil, nil, 0, fmt.Errorf("failed to decode key size")
	}
	index += n
	// 确保读取的字节数足够处理 keySize
	if index >= len(buf) {
		return nil, nil, 0, fmt.Errorf("buffer too small to contain key size")
	}
	// 解码值的大小
	valueSize, n := binary.Uvarint(buf[index:])
	if n <= 0 {
		return nil, nil, 0, fmt.Errorf("failed to decode value size")
	}
	index += n

	kvSize := int(keySize + valueSize)

	kvBuf := make([]byte, kvSize)

	// 读取键值对数据 并进行错误处理
	cnt, err = r.ReadAt(kvBuf, int64(index+offset))
	if err != nil {
		return nil, nil, 0, err
	}
	if cnt == 0 {
		return nil, nil, 0, io.EOF
	}
	// 读取key和value
	key, value := kvBuf[:keySize], kvBuf[keySize:]
	buf = buf[:index]
	buf = append(buf, key...)
	buf = append(buf, value...)
	// 校验crc32
	realCrc32 := crc32.ChecksumIEEE(buf[crc32.Size:])
	if realCrc32 != expectCrc32 {
		return nil, nil, 0, fmt.Errorf("crc check err")
	}
	return kvBuf[:keySize], kvBuf[keySize:], index + kvSize, nil
}
func (w *WalReader) size() int {
	stat, err := w.wal.Stat()
	if err != nil {
		panic(err)
	}
	return int(stat.Size())
}
func (w *WalReader) reader(off int) ([]byte, []byte, int, error) {
	return readData(w.wal, off, w.size())
}
