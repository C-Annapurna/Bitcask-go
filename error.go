package bitcask_go

import "errors"

// 以下定义了几种常见的错误
var (
	ErrKeyisEmpty              = errors.New("the key is empty")
	ErrIndexUpdataFailed       = errors.New("failed to update index")
	ErrKeyNotFound             = errors.New("key not found in database")
	ErrDataFileNotFound        = errors.New("data file is not found")
	ErrDatatDirectoryCorrupted = errors.New("the database directory maybe coorupted")
	ErrExceedMaxBatchNum       = errors.New("exceed the max batch num")
	ErrMergeIsProcess          = errors.New("merge is in process, try again later")
	ErrDatabaseIsUsing         = errors.New("the database directory is used by another process")
	ErrMergeRatioUnreached     = errors.New("the merge ratio do not reach options")
	ErrNoEnoughSpaceForMerge   = errors.New("no enough space for merge")
)
