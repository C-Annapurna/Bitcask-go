package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/fio"
	"bitcask-go/index"
	"bitcask-go/utils"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// bitcask存储引擎结构(供用户使用)   这个引擎会将磁盘上的数据读取到内存中，并且在内存中维护一个索引结构
type DB struct {
	options         Option //初始化数据库的一些配置
	mu              *sync.RWMutex
	fileIds         []int                     //文件id(已排序)，只能在加载索引的时候使用，不能在其他地方更新或者修改
	activeFile      *data.DataFile            //当前活跃文件，保存着索引信息。可以用于写入append   里面有文件id，有文件偏移，有io_manager(用于向磁盘中进行操作的read、write、sync、close)
	olderFile       map[uint32]*data.DataFile //旧数据文件，只能用于读      在这里activeFile和olderFile文件的编号FileId 都是由DirPath目录下.data文件的编号决定的
	index           index.Indexer             //数据内存索引   对索引进行操作的
	seqNo           uint64                    //事务序列号 全局递增   针对writebatch的   在batch.go中，commit操作时会进行递增的
	isMerging       bool                      //是否正在进行merge操作
	seqNoFileExists bool                      //存储事务序列号的文件是否存在
	isInitial       bool                      //是否第一次初始化此数据目录
	fileLock        *flock.Flock              //文件锁保证多进程之间的互斥(保证当前只有一个存储引擎打开数据目录)
	bytesWrite      uint                      //标识当前已经写了多少个字节   与配置项中bytespersync互帮互助
	reclaimSize     int64                     //表示有多少数据是无效的
}

// Stat 存储引擎统计信息
type Stat struct {
	KeyNum          uint  //存储引擎中key的总数量
	DataFileNum     uint  //数据文件总数量
	ReclaimableSize int64 //可以进行merge回收的数据量，字节为单位
	DiskSize        int64 //数据目录所占磁盘空间的大小
}

// 定义一个打开bitcask存储引擎实例的方法
func OpenDB(options Option) (*DB, error) {
	//对用户传入的配置项进行校验，检查文件路径和文件大小这样的设置是不是对的
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool //这个参数表示是否是第一次初始化

	//对用户传递进来的目录进行校验     如果目录不存在就创建这个目录    该目录就是存放.data文件的地方
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil { //os.ModePerm为0777，表示最大的读写权限
			return nil, err
		}
	}

	//判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName)) //初始化一个文件锁
	hold, err := fileLock.TryLock()                                     //尝试获取这把锁
	if err != nil {
		return nil, err
	}
	if !hold { //这个布尔值就表示是否获得了锁，没有获得就表示当前文件夹被其他进程打开了，直接返回错误类型就好
		return nil, ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(options.DirPath) //读取该目录下的所有文件
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	//初始化db实例的结构体
	db := &DB{
		options:   options,
		mu:        new(sync.RWMutex),
		olderFile: make(map[uint32]*data.DataFile),
		index:     index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites), //这里的index涉及到内存索引的一些操作
		isInitial: isInitial,
		fileLock:  fileLock,
	}

	//加载merge数据目录  经过这一步，就将merge临时文件中的内容都转移到原数据库的数据文件夹中了
	if err := db.loadMergeFile(); err != nil {
		return nil, err
	}

	//加载对应的数据文件  将磁盘上的文件加载到db实例的activeFile和olderFile中   注意activeFile和olderFile中的*data.DataFile是能够使用抽象接口IOManeger对磁盘上数据进行操作的
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	//如果是b+树的结构，就不需要使用下面加载索引的方式了，直接从磁盘加载索引
	if options.IndexType != BPLusTree {
		//从hint索引文件中加载索引
		if err := db.loadIndexFromHint(); err != nil {
			return nil, err
		}

		//从数据文件当中加载索引
		if err := db.loadIndexerFromDataFile(); err != nil {
			return nil, err
		}

		//充值IO类型为标准文件   因为本次课程只是用mmap进行启动加速，不涉及读
		if db.options.MMapAtStartup {
			if err := db.resetIOType(); err != nil {
				return nil, err
			}
		}

	}

	//取出当前事务序列号
	if options.IndexType == BPLusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	return db, nil
}

func (db *DB) Close() error {
	defer func() { //在最后关闭数据库的时候，别忘了释放文件锁
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory, %v", err))
		}
	}()
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	//关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}

	//保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath) //这是在dirpath下的"seq-no"文件中保存当前事务序列号
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),                         //这个key表示当前是一个表示事务序列号的键值
		Value: []byte(strconv.FormatUint(db.seqNo, 10)), //这里将无符号整数db.seqNo转化为字符串表示形式
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil { //将当前保存事务序列号的信息保存在磁盘上
		return err
	}

	//关闭当前的活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	//关闭旧的数据文件
	for _, file := range db.olderFile {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// 返回数据库的相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFile))
	if db.activeFile != nil {
		dataFiles += 1
	}
	dirSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : &v", err))
	}

	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        dirSize,
	}
}

// backup 备份数据库，将数据文件拷贝到新的目录中
func (db *DB) BackUp(dir string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}

// 向DB的activaFile中append写入key/value数据，key不能为空
// Put的流程：1、写入数据库中    2、更新内存索引
func (db *DB) Put(key []byte, value []byte) error {
	//判断key是否有效
	if len(key) == 0 {
		return ErrKeyisEmpty
	}

	//构造logRecord结构体
	logRecord := &data.LogRecord{
		logRecordKeyWithSeq(key, nonTransactionSeqNo),
		value,
		data.LogRecordNormal,
	}

	//追加写入到当前的活跃数据文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	//程序运行到这里就能拿到我们的索引信息
	//更新内存索引		内存索引更新之后，写数据流程就完成了
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

// 根据key删除对应的数据    主要添加的逻辑就是要判断希望删除的key是否有效，不然免得无关的数据文件膨胀
func (db *DB) Delete(key []byte) error {
	//判断key的有效性
	if len(key) == 0 {
		return ErrKeyisEmpty
	}

	//先检查key是否存在，如果不存在直接返回
	if pos := db.index.Get(key); pos == nil {
		return ErrKeyNotFound
	}

	//运行到这里表示校验通过了，我们删除的是有效的key
	//构造LogRecord，标识其被删除
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	//写入到数据文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size)

	//然后在对应的内存索引当中将其删除掉    在内存索引的操作还是比较好实现的
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdataFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
	}
	return nil
}

// 根据key读取数据，这一步的逻辑比较好实现
func (db *DB) Get(key []byte) ([]byte, error) {
	//在读取的时候要注意锁的保护，注意是可以多个进程一起读的，所以我们加读锁，多个goroutine可以同时获得读锁，以便并发读取共享资源
	db.mu.RLock()
	defer db.mu.RUnlock()

	//1、判断key是否为空
	if len(key) == 0 {
		return nil, ErrKeyisEmpty
	}

	//2、从内存数据结构中取出key对应的索引信息
	logRecordPos := db.index.Get(key)
	//如果key不在内存索引中，说明key不存在
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	//从数据文件中获取value
	return db.getValueByPosition(logRecordPos)
}

// 获取数据库中所有的key   这里的listkeys有问题啊，不能成功将所有key列出来
func (db *DB) ListKeys() [][]byte {
	//先得到迭代器
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
	}
	return keys
}

// 获取所有的数据，并执行用户指定的函数操作，函数返回false时终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// 根据索引的信息得到对应的value
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	//3、程序运行到这里表示有对应的索引文件，根据索引信息查找
	var dataFile *data.DataFile
	if logRecordPos.Fid == db.activeFile.FileId { //如果要查找的文件在数据库的活跃文件中
		dataFile = db.activeFile
	} else { //只能去oldFile中进行查找
		dataFile = db.olderFile[logRecordPos.Fid]
	}

	//数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	//4、程序运行到这一步表示读取到了对应的数据文件，需要根据logRecordPos的偏移量读取对应的文件
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted { //如果当前文件其实已被删除
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

//Close关闭数据库    将文件描述符(活跃文件+旧文件关闭)

//Sync持久化数据库将数据文件在缓冲区的内容刷到磁盘，保证数据不丢失
//只需要sync当前活跃文件就行了，旧数据文件在当时代码逻辑中已经进行sync了  体现在appendLogRecord中

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(logRecord)
}

// 追加写数据到活跃文件中
// 这里涉及多线程并发写入的问题，需要加锁保护   输入参数logRecord是写入的数据    返回数据写入磁盘的位置以及error信息
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	//判断当前活跃文件是否存在，因为数据库在没有写入数据或者刚启动的时候是没有文件生成的
	//如果不存在则需要初始化
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//程序运行到此处，我们就有了自己的活跃文件，可以对该活跃文件添加文件了
	encRecord, size := data.EncodeLogRecord(logRecord)

	//在这里需要进行一个判断，如果写入的数据已经达到了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		//在进行文件状态转换的时候需要对当前活跃文件进行持久化，保证已有的文件被持久化到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		//持久化之后需要将当前的活跃文件转化为旧的活跃文件
		db.olderFile[db.activeFile.FileId] = db.activeFile

		//再打开一个新的活跃文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//程序运行到这一步，也就该开始进行写入的操作了
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	//根据用户配置决定是否进行持久化
	db.bytesWrite += uint(size)
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrite >= db.options.BytesPerSync {
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		//清空累计值
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	//构造内存索引信息并返回
	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// 这个函数的功能是设置活跃的数据文件(可append的)
// 在访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1 //每一个数据文件在新建的时候，id都是递增的
	}

	//打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId, fio.StanderdFIO) //在这里面应该注意实现的时候，writeOff也需要更新
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 根据options中的参数从磁盘中加载对应的数据文件   加载到db实例的activeFile和olderFile中
func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.options.DirPath) //使用go提供的包，得到db.options.DirPath目录下的所有文件信息
	if err != nil {
		return err
	}

	var fileIds []int //这个数组就是用来统计db.options.DirPath中的所有id的
	//遍历目录当中的所有文件，只需要以.data为后缀的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) { //如果当前文件的文件名后缀为.data
			//文件名命令格式：000000000.data    取名称前面的部分作为我们文件的id
			splitName := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitName[0]) //这里实现的功能就是将字符串转换为整数
			if err != nil {
				return ErrDatatDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId) //将读取到的文件id号append到数组中
		}
	}
	//然后对文件id进行一波排序，从小到达依次加载
	sort.Ints(fileIds)
	db.fileIds = fileIds

	//遍历每一个文件id，打开对应的数据文件
	for i, fid := range fileIds {
		ioType := fio.StanderdFIO
		if db.options.MMapAtStartup {
			ioType = fio.MemoryMap
		}

		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType) //此时得到的dataFile里面有IOManeger，能够实现对磁盘上的数据进行操作
		if err != nil {
			return err
		}

		//如果遍历到了最后一个文件，我们会把这个文件放置到activeFile活跃文件中 否则放置到olderFile中
		if i == len(fileIds)-1 { //表示当前的dataFile应该存放在activateFile中
			db.activeFile = dataFile
		} else { //说明应该存放在旧文件中
			db.olderFile[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引
// 遍历数据文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexerFromDataFile() error {
	//没有文件，说明数据库是空的，直接返回就可以了   也就是说磁盘文件里面没*.data文件
	if len(db.fileIds) == 0 {
		return nil
	}

	//查看是否发生过merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid //得到还没有进行merge操作的文件的id
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) { //定义一个匿名函数，对每一段数据进行处理，如果是已经删除了，就在内存索引中删掉
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(oldPos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	//暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionReocrd)
	var currentSeqNo = nonTransactionSeqNo

	//遍历所有的文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		//如果当前文件id比nonMergeFileId小，则说明当前文件的索引已经在更新hint文件时被更新了，不需要重复更新
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId { //当前文件是活跃文件
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFile[fileId]
		}

		//拿到对应的数据文件之后,就需要循环的处理这个文件当中的所有内容  也就是将该文件中的每一条记录都放置在内存索引中
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset) //根据这个偏移量，从dataFile中读取出logRecord项
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//构造内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset, Size: uint32(size)}

			//解析key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				//非readBatch提交的事务，则直接更新
				updateIndex(realKey, data.LogRecordNormal, logRecordPos)
			} else {
				//事务操作，需要判断该事务是否已完成
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo) //清空暂存数据
				} else {
					//当前读到的事务数据中还没有读到最后一个
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionReocrd{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}

			}
			//更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			//递增offset，下一次从新的位置开始读取
			offset = offset + size
		}

		// 这里的i == len(db.fileIds)-1 表示达到了读取的磁盘文件的最后一项，我们通常将这一项设置为activaFile
		// 如果当前是活跃文件的话，就需要对activeFile中的WriteOff进行更新   表示下一次写应该从哪里开始
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	//最后更新事务序列号
	db.seqNo = currentSeqNo

	return nil
}

func checkOptions(options Option) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database file size must be greater than 0")
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio,must be between 0 and 1")
	}
	return nil
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true
	return nil
}

// 将数据文件的IO类型重新设置为标准文件IO
func (db *DB) resetIOType() error {
	if db.activeFile == nil {
		return nil
	}
	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StanderdFIO); err != nil {
		return err
	}
	for _, dataFile := range db.olderFile {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StanderdFIO); err != nil {
			return err
		}
	}
	return nil
}
