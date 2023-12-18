package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/utils"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "_merge"         //这个用来新建一个临时文件夹用于merge的
	mergeFinishedKey = "merge.finished" //这个标识当前merge过程结束
)

// 清理无效数据，生成Hint文件
// Merge完成的操作主要就是，会在磁盘上新建一个merge的临时目录，主要将olderfile遍历，同时根据db的内存索引进行比较，将好的数据先复制粘贴过来，同时生成hint文件
// 这个时候，不影响原来的db继续在activefile上进行读写操作，完成数据的清理之后，再将临时文件上的内容copy到原数据库文件目录中
func (db *DB) Merge() error {
	//如果数据库为空，直接返回
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	//如果merge正在进行当中，直接返回即可
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProcess
	}

	//查看可以merge的数据量是否达到了阈值
	totalSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if float32(db.reclaimSize)/float32(totalSize) < db.options.DataFileMergeRatio {
		db.mu.Unlock()
		return ErrMergeRatioUnreached
	}

	//查看当前磁盘剩余空间是否可以容纳merge之后的数据量
	availableDiskSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.mu.Unlock()
		return err
	}
	if uint64(totalSize-db.reclaimSize) >= availableDiskSize {
		db.mu.Unlock()
		return ErrNoEnoughSpaceForMerge
	}

	db.isMerging = true
	defer func() { //自定义一个匿名函数让Merge的最后将这个字段重新设为false   merge结束之后让这个字段变为false
		db.isMerging = false
	}()

	//接下来直接进行merge的流程
	//总的merge流程：1、对当前活跃文件进行处理(持久化并转为旧的文件)，然后打开新的活跃文件;
	//				2、取出所有需要merge的文件
	//				3、新建一个mergeDB，用于对需要merge的文件进行处理
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	//将当前活跃文件转换为旧的活跃文件
	db.olderFile[db.activeFile.FileId] = db.activeFile
	//再打开一个新的活跃文件，用户将此后的操作在这个新的活跃文件上进行
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	nonMergeFileId := db.activeFile.FileId //记录当前活跃文件的id，标识这是最近的没有参与merge的数据文件

	//取出所有需要的merge的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFile {
		mergeFiles = append(mergeFiles, file)
	}
	//将所有的olderFile存放在mergeFiles中，然后接下来就只需要对mergeFiles进行merge操作就行了
	db.mu.Unlock()

	//待merge的文件进行从小到达进行排序，依次merge   这里是对mergeFiles进行排序，使用匿名函数作为比较函数
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	//判断当前目录是否存在，是的话要将里面的内容删除掉    不是的话就新建这样一个目录
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	//新建一个merge path目录     os.ModePerm标识777，最大的读、写、执行权限
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	//打开一个新的临时bitcask实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false //中途merge的时候万一失败了，我们直接认为本次merge失败，不需要使用sync操作
	mergeDB, err := OpenDB(mergeOptions)
	if err != nil {
		return err
	}

	//打开Hint文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	//遍历处理每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF { //当前数据文件已经读完了
					break
				}
				return err
			}
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			//这里判断文件是否有效的逻辑：位置信息不能为空    数据文件id得对得上     偏移量也得对得上    无效的话就直接跳过了
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				//	清楚事务标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				//将当前位置索引写到hint文件中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			//递增offset
			offset += size
		}
	}

	//sync保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}

	if err := mergeDB.Sync(); err != nil {
		return err
	}
	//写表示merge完成的文件   写在当前的activeFile中
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))), //这里就记录最近没有参与merge的文件的id
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}
	return nil
}

// 针对当前存储引擎的目录进行merge
// 需要的结构/tmp/bitcask
//
//	/tmp/bitcask_merge
func (db *DB) getMergePath() string {
	//dir := path.Dir(path.Clean(db.options.DirPath)) //clean表示将多余的斜杠去掉    Dir函数作用是拿到父目录
	base := path.Base(db.options.DirPath) //拿到名字，比如 /tmp/bitcask这个目录，就会返回bitcask这个名字
	//return filepath.Join(dir, base+mergeDirName)
	return filepath.Join(base + mergeDirName)
}

// 加载merge数据目录
func (db *DB) loadMergeFile() error {
	mergePath := db.getMergePath() //经过这一段得到的mergePath，输入到os.ReadDir中时，会报错 得到的字符串：.\C:\Users\16005\AppData\Local\Temp\bitcask-go-http3499270860_merge
	//mergePath := "G:\\GO_Project\\kv_project\\tmp\\http_merge"
	//merge目录不存在的话直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}() //后面还会进行移除文件的操作

	//接下来就是整个merge目录都存在，需要简化merge数据读取出来
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	//查看表示merge完成的文件，判断merge是否完成了
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName { //data.MergeFinishedFileName为"merge-finished"
			mergeFinished = true
		}
		if entry.Name() == data.SeqNoFileName { //"seq-no"
			continue
		}
		if entry.Name() == fileLockName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	if !mergeFinished { //读完了merge目录下的文件也没读到merge完成的标识，标识上一次merge出错了，直接返回就好了
		return nil
	}

	//接下来就是使用merge完成之后的文件替换掉原来的olderFile    得到最近没有进行merge操作的文件
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	//删除旧的数据文件  只能删除id比nonMergeFileId更小的数据文件，  id比nonMergeFileId大表示这是merge发生之后新增的数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			} //如果数据文件存在就删除掉
		}
	}

	//将新的数据文件(merge之后的文件)移动到数据目录中
	for _, fileName := range mergeFileNames {
		//将  /tmp/bitcask-merge   00.data  11.data改为
		//    /tmp/bitcask         00.data  11.data
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

// 从hint文件中加载索引
func (db *DB) loadIndexFromHint() error {
	//首先查看hint索引文件是否存在
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) { //这里表示当前文件下没有hint文件
		return nil
	}

	//打开hint索引文件
	hintFile, err := data.OpenHintFile(hintFileName)
	if err != nil {
		return err
	}

	//读取文件中的索引  ，并存放在index中
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		//解码得到实际的位置索引
		pos := data.DecodeLogRecordPos(logRecord.Value) //这里hint文件中的value是实际的数据文件位置，所以进行解码得到pos
		db.index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil

}
