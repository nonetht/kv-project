package bitcask_go

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("key not found")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDataDirectoryCorrupted = errors.New("data directory corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed the max batch")
	ErrMergeIsProgress        = errors.New("merge is in progress, try it later")
)
