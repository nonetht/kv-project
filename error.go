package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("key is empty")
	ErrIndexUpdateFailed      = errors.New("fail to update index")
	ErrKeyNotFound            = errors.New("key not found")
	ErrDataFileNotExist       = errors.New("data file not exist")
	ErrDataDirectoryCorrupted = errors.New("database directory corrupted")
)
