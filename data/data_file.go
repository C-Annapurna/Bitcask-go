package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid CRC value,log record maybe corrupted")
)

const (
	DataFileNameSuffix    = ".data"
	HintFileName          = "hint-index"
	MergeFinishedFileName = "merge-finished"
	SeqNoFileName         = "seq-no"
)

// 数据文件的一些字段
type DataFile struct {
	FileId    uint32        //文件id   用于表明当前DataFile的编号
	WriteOff  int64         //文件偏移：应该把文件写到当前DataFile文件的哪个位置
	IoManager fio.IOManager //io读写管理   这里就涉及将数据写入到磁盘(数据库)中
}

// 打开新的数据文件
func OpenDataFile(dirpath string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	//根据传入的dirpath和fileId，加上后缀.data之后  我们就找到了对应在磁盘上的文件，然后根据文件 填充好DataFile这个结构体，然后返回就好了
	fileName := GetDataFileName(dirpath, fileId)
	//这样就得到了文件的路径  G://....//000000000.data
	return newDataFile(fileName, fileId, ioType)

}

// 打开Hint索引文件
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName) //HintFileName为"hint-index"
	return newDataFile(fileName, 0, fio.StanderdFIO)
}

// 与打开Hint文件类似，添加一个标识merge完成的文件
func OpenMergeFinishedFile(dirpath string) (*DataFile, error) {
	fileName := filepath.Join(dirpath, MergeFinishedFileName)
	return newDataFile(fileName, 0, fio.StanderdFIO)
}

// 存储事务序列号的文件
func OpenSeqNoFile(dirpath string) (*DataFile, error) {
	fileName := filepath.Join(dirpath, SeqNoFileName)
	return newDataFile(fileName, 0, fio.StanderdFIO)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func newDataFile(fileName string, fileId uint32, ioType fio.FileIOType) (*DataFile, error) {
	ioManager, err := fio.NewIOManager(fileName, ioType) //所以IOManager是针对磁盘上的文件进行操作的
	if err != nil {
		return nil, err
	}

	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
	//这样返回的DataFile就能够对G://....//000000000.data文件进行read write等操作了
}

// header信息：crc验证码：4字节   Type：1字节  keysize：5字节  valuesize：5字节
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size() //返回当前dataFile文件的总大小
	if err != nil {
		return nil, 0, err
	}

	//如果读取的最大header长度已经超过了文件的长度，则只需要读取到文件末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize //15
	if int64(maxLogRecordHeaderSize)+offset > fileSize {
		headerBytes = fileSize - offset
	}

	//在读取数据以及启动引擎实例的时候都需要使用，实现的作用就是根据偏移offset读取指定位置的logrecord信息
	headerBuf, err := df.readNBytes(headerBytes, offset) //从offset开始处读取前headerBytes个字节的数据
	if err != nil {
		return nil, 0, err
	}

	//拿到了header信息之后，需要对它进行解码
	header, headerSize := decodeLogRecordHeader(headerBuf) //解码读取出来的headerBuf信息
	//以下两个条件表示读取到了文件末尾，直接返回EOF错误
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	//取出key和value的长度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize //这里记录的就是整个logrecord的长度
	//整个record的形状是： crc   type   keysize    valuesize    key   value

	logRecord := &LogRecord{
		Type: header.recordType,
	}

	//开始读取用户实际存储的key/value
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize) //这里读取的就是key和value的数据
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	//最后校验以下数据的crc是否正确     crc需要根据type字段   keysize字段   value字段共同计算得到
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	//注意这里的headerBuf的总长度是maxLogRecordHeaderSize，但是实际上有效的数据长度只有headerSize，需要进行一次截取
	//这里传入的第二个参数就是header中的keysize和valuesize字段

	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

func (df *DataFile) Sync() error { //将文件持久化到磁盘当中
	return df.IoManager.Sync()
}

// 写入索引信息到hint文件中
func (df *DataFile) WriteHintRecord(key []byte, pos *LogRecordPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) Write(buf []byte) error { //写入到数据文件中
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n) //写入数据之后要记得更新DataFile中的偏移量
	return nil
}

func (df *DataFile) SetIOManager(dirPath string, ioType fio.FileIOType) error {
	if err := df.IoManager.Close(); err != nil {
		return err
	}
	ioManager, err := fio.NewIOManager(GetDataFileName(dirPath, df.FileId), ioType)
	if err != nil {
		return err
	}
	df.IoManager = ioManager
	return nil
}

// 下面函数就是指定从datafile的offset处开始读取前n字节的信息
func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset) //使用接口实现的Read函数来读取对应的字节流
	return
}
