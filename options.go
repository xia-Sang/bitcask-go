package bitcask

import "os"

type Options struct {
	DirPath     string //文件地址
	MaxFileSize int64  //单个文件最大容量
}

func mkdirPath(dirPath string) error {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return err
		}
	}
	return nil
}
func (opts *Options) check() error {
	if err := mkdirPath(opts.DirPath); err != nil {
		return err
	}
	return nil
}

// NewOptions 配置文件构造器
func NewOptions(dirPath string, opts ...ConfigOptions) *Options {
	op := Options{
		DirPath: dirPath,
	}
	for _, opt := range opts {
		opt(&op)
	}

	defaultOptions(&op)
	if err := op.check(); err != nil {
		panic(err)
	}
	return &op
}

// ConfigOptions 配置数据
type ConfigOptions func(*Options)

func WithMaxLevel(level int) ConfigOptions {
	return func(o *Options) {
		o.DirPath = "./data"
	}
}
func WithSSTSize(sstSize uint64) ConfigOptions {
	return func(o *Options) {
		o.MaxFileSize = 1024
	}
}
func defaultOptions(opts *Options) {
	if opts.MaxFileSize == 0 {
		opts.MaxFileSize = 1024
	}
}
