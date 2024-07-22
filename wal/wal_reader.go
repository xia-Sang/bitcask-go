package wal

import (
	"encoding/binary"
	"fmt"
	"github.com/xia-Sang/bitcask/memtable"
	"hash/crc32"
	"io"
)

func (w *Wal) Read(table memtable.MemTable) error {
	offset := 0
	for {
		key, value, length, err := w.ReadAt(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if value != nil {
			table.Put(key, &Pos{
				FileId: w.FileId,
				Offset: offset,
				Length: length,
			})
		} else {
			table.Delete(key)
		}
		offset += length
	}
	return nil
}

// 读取时候我们只需要给出readat 和 offset即可
// 并不需要长度信息的
func readData(r io.ReaderAt, offset int) ([]byte, []byte, int, error) {
	// var headerSize = BufferSize
	// if size < BufferSize+Offset {
	// 	headerSize = size - Offset //最后的剩余容量 go语言的特性是 完全不需要这样来实现的
	// }
	buf := make([]byte, BufferSize)
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
	keySize, n := binary.Varint(buf[index:])
	if n <= 0 {
		return nil, nil, 0, fmt.Errorf("failed to decode key size")
	}
	index += n
	// 确保读取的字节数足够处理 keySize
	if index >= len(buf) {
		return nil, nil, 0, fmt.Errorf("buffer too small to contain key size")
	}
	// 解码值的大小
	valueSize, n := binary.Varint(buf[index:])
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

// 直接是定长读取
func readDataWithLength(r io.ReaderAt, offset int, length int) ([]byte, []byte, error) {
	buf := make([]byte, length)
	cnt, err := r.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, nil, err
	}
	if cnt == 0 {
		return nil, nil, io.EOF
	}
	index := crc32.Size
	// 读取并忽略 CRC 校验码
	expectCrc32 := binary.BigEndian.Uint32(buf[:index])
	// 解码键的大小
	keySize, n := binary.Varint(buf[index:])
	if n <= 0 {
		return nil, nil, fmt.Errorf("failed to decode key size")
	}
	index += n
	// 确保读取的字节数足够处理 keySize
	if index >= len(buf) {
		return nil, nil, fmt.Errorf("buffer too small to contain key size")
	}
	// 解码值的大小
	valueSize, n := binary.Varint(buf[index:])
	if n <= 0 {
		return nil, nil, fmt.Errorf("failed to decode value size")
	}
	index += n

	//fmt.Println("index", index, keySize, valueSize, length)
	// 读取key和value
	key, value := buf[index:index+int(keySize)], buf[index+int(keySize):index+int(keySize+valueSize)]
	buf = buf[:index]
	buf = append(buf, key...)
	buf = append(buf, value...)
	// 校验crc32
	realCrc32 := crc32.ChecksumIEEE(buf[crc32.Size:])
	if realCrc32 != expectCrc32 {
		return nil, nil, fmt.Errorf("crc check err")
	}
	return key, value, nil
}

func (w *Wal) Size() int64 {
	stat, err := w.wal.Stat()
	if err != nil {
		panic(err)
	}
	return stat.Size()
}

// ReadAt 直接读取即可
func (w *Wal) ReadAt(off int) ([]byte, []byte, int, error) {
	return readData(w.wal, off)
}

// ReadBuf 按照指定长度读取
func (w *Wal) ReadBuf(off, length int) ([]byte, []byte, error) {
	return readDataWithLength(w.wal, off, length)
}
