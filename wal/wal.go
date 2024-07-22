package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"

	"github.com/xia-Sang/bitcask/memtable"
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
func TestWal() (*Wal, error) {
	dirPath := "./test"
	fileId := 1
	filePath := path.Join(dirPath, GetWalPath(fileId))
	if _, err := os.Stat(filePath); err != nil {
		if err := os.Mkdir(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	fp, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &Wal{FileId: fileId, wal: fp, dirPath: dirPath}, nil
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
