package bitcask_go

import (
	"bitcask-go/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin") //定义一个结束标识，表示事务完成

// 原子批量写数据，保持原子性
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord //暂存用户写入的数据
}

// 初始化WriteBatch的方法   将批量化的数据存放在pendingWrites中，到时候统一的更新在内存以及磁盘上    这里是新建一个事务实例
func (db *DB) NewWrietBatch(opts WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPLusTree && !db.seqNoFileExists && !db.isInitial { //这里有b+树的特点，我也忘了，好像索引结构是存放在磁盘上的
		panic("cannot use write batch,seq number not exists")
	}
	return &WriteBatch{
		options:       opts,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyisEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//暂存LogRecord
	logRecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// 批量删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyisEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//数据不存在则直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key)) //从map中删除指定的key
		}
		return nil
	}

	//也是将数据先暂存起来
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted} //表示为deleted
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// 提交事务，将暂存的数据写到数据文件，并更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 { //writebatch操作中没有数据
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum { //超过配置限制
		return ErrExceedMaxBatchNum
	}

	//这里加锁保证事务提交的串行化(下面涉及到对db中全局递增变量seqNo进行加一的操作)
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	//接下来就是实际的写入数据
	//首先获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1) //原子操作来递增一个无符号整数（uint64）变量，并将递增后的值赋给变量seqNo

	positions := make(map[string]*data.LogRecordPos) //用来保存将事务的logrecord存放的位置   后续将用于内存索引更新

	//开始写数据到数据文件中
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{ //注意此前db.appendLogRecord函数内部已经上锁了，所以我们更改了一下db.go中的源码，添加了db.appendLogRecordWithLock的逻辑
			Key:   logRecordKeyWithSeq(record.Key, seqNo), //将序列号也编码到key中
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = logRecordPos
	}

	//前面的提交都已成功需要加上一条事务完成的标志
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	_, err := wb.db.appendLogRecord(finishedRecord)
	if err != nil {
		return err
	}

	//根据我们的配置进行持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	//更新对应的内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.LogRecordNormal {
			oldPos = wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}

	//清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)
	return nil
}

// 将key+seq number进行编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo) //将一个无符号整数编码为字节序列，并将结果存储在seq数组中
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

// 解析传入的key，拿到实际的key还有seqNo事务序列号的部分
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
