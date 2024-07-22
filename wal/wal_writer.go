package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
)

const (
	BufferSize  = 4 + 2*binary.MaxVarintLen64
	WalFileName = ".wal"
)

// Wal 这个里面是不断地进行追加写操作
type Wal struct {
	dirPath string
	FileId  int //file_id
	Offset  int //写指针
	wal     *os.File
}
type Pos struct {
	FileId int
	Offset int
	Length int
}

func (w *Wal) CloseAndDelete() error {
	if err := w.wal.Close(); err != nil {
		return err
	}
	return os.Remove(path.Join(w.dirPath, GetWalPath(w.FileId)))
}
func GetWalPath(fileId int) string {
	return fmt.Sprintf("%08d%s", fileId, WalFileName)
}
func NewWal(dirPath string, fileId int) (*Wal, error) {
	filePath := GetWalPath(fileId)
	fp, err := os.OpenFile(path.Join(dirPath, filePath), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &Wal{FileId: fileId, wal: fp, dirPath: dirPath}, nil
}
func (w *Wal) SetOffset(offset int) error {
	_, err := w.wal.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return err
	}
	return nil
}
func (w *Wal) Write(key, value []byte) (*Pos, error) {
	length, err := write(w.wal, key, value)
	if err != nil {
		return nil, err
	}
	pos := &Pos{
		FileId: w.FileId,
		Offset: w.Offset,
		Length: length,
	}
	// 更新offset
	w.Offset += length
	return pos, nil
}
func (w *Wal) Sync() error {
	return w.wal.Sync()
}
func (w *Wal) Close() error {
	return w.wal.Close()
}
func (w *Wal) WriteAt(offset int, key, value []byte) (int, error) {
	return writeAt(w.wal, offset, key, value)
}
func writeAt(w io.WriterAt, offset int, key, value []byte) (int, error) {
	buf := buff(key, value)
	// 写入缓冲区到写入器
	if _, err := w.WriteAt(buf, int64(offset)); err != nil {
		return 0, err
	}

	return len(buf), nil
}

// 顺序写入
func write(w io.Writer, key, value []byte) (int, error) {
	buf := buff(key, value)
	// 写入缓冲区到写入器
	if _, err := w.Write(buf); err != nil {
		return 0, err
	}

	return len(buf), nil
}
func buff(key, value []byte) []byte {
	keySize := len(key)
	valueSize := len(value)
	totalSize := BufferSize + keySize + valueSize

	buf := make([]byte, totalSize)
	index := crc32.Size

	// 存储键值对大小
	index += binary.PutVarint(buf[index:], int64(keySize))
	index += binary.PutVarint(buf[index:], int64(valueSize))

	// 存储键和值
	copy(buf[index:], key)
	index += keySize
	copy(buf[index:], value)
	index += valueSize
	// 计算并存储 CRC 校验码
	crc := crc32.ChecksumIEEE(buf[crc32.Size:index])

	binary.BigEndian.PutUint32(buf[:crc32.Size], crc)
	return buf[:index]
}
