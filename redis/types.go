package redis

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

// redis数据结构服务
type RedisDataStructure struct {
	db *bitcask.DB
}

// 初始化redis数据结构服务
func NewRedisDataStructure(options bitcask.Option) (*RedisDataStructure, error) {
	db, err := bitcask.OpenDB(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

func (rds *RedisDataStructure) Close() error {
	return rds.db.Close()
}

// ===================string数据结构=======================
func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	//编码value：   type + expire(过期时间) + payload(原始value)
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano() //如果ttl不为0的话，我们将当前时间加上ttl得到过期时间

	}
	index += binary.PutVarint(buf[index:], expire) //编码过期时间字段，此时index表示type和expire字段加起来的长度
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	//编码完成之后调用存储引擎接口写入
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	//接下来就是进行解码
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	if expire > 0 && expire <= time.Now().UnixNano() { //time.Now().UnixNano()返回当前时间纳秒级别的时间戳
		return nil, nil
	}
	return encValue[index:], nil //到这就表示数据类型是String，且数据没有过期
}

// ===================Hash数据结构=======================
// 注意hash的执行过程：
// 首先是根据传入的key   ---->   元数据(type+expire+version+size)
// 然后从元数据和传入的key中得到encKey(key+version+field)   ---->   最后的value
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	//先查找元数据
	meta, err := rds.findMetadata(key, Hash) //先得到元数据
	if err != nil {
		return false, err
	}

	//注意在hash结构中，使用的key实际上是key+version+field的组合，所以这里需要得到encKey
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode() //得到encKey(包含key+version+field)

	//先查找是否存在
	var exist = true
	if _, err := rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	wb := rds.db.NewWrietBatch(bitcask.DefaultWriteBatchOptions) //由于可能涉及到key-meta的写入，encKey-value的写入。所以使用writebatch进行处理

	//不存在则更新元数据
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode()) //将meta表示为字节流写入到引擎中
	}
	wb.Put(encKey, value) //encKey(key+version+field)和value对应起来
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

// 用户会传递，根据key和field找到对应的数据
func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil //当前元数据里面根本没有东西，直接返回
	}

	//使用meta当中的key+version+field组成hashInternalKey，然后用存储引擎接口方法直接获取数据即可
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	return rds.db.Get(hk.encode())
}

// 删除数据
func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil //当前元数据里面根本没有东西，直接返回
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()

	//首先查看是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rds.db.NewWrietBatch(bitcask.DefaultWriteBatchOptions)
		meta.size--
		wb.Put(key, meta.encode())
		wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, nil
		}
	}
	return exist, nil
}

// ===================Set数据结构=======================
func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	//首先查找对应的元数据
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	//构造一个数据部分的key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	//接下来就是调用存储引擎的接口查看member是否存在(注意这里输入的是sk,包括了key,version和member信息)
	if _, err = rds.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound {
		//不存在的话则更新.注意更新的时候使用writebatch保证操作的原子性
		wb := rds.db.NewWrietBatch(bitcask.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil) //这里的数据部分是nil,原理在此前讲过,先做个记号,以后再说
		if err = wb.Commit(); err != nil {
			return false, err
		}
		ok = true

	}
	return ok, nil
}

// 判断一个member是否属于某一个key下面
func (rds *RedisDataStructure) SisMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 { //表示虽然key存在但是里面没内容,直接返回即可
		return false, nil
	}

	//构造一个数据部分的key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	//接下来就是在存储引擎中进行查找
	_, err = rds.db.Get(sk.encode())
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

// 删除一个member
func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}
	if meta.size == 0 { //表示虽然key存在但是里面没内容,直接返回即可
		return false, nil
	}

	//构造一个数据部分的key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound { //不存在
		return false, nil
	}

	//更新
	wb := rds.db.NewWrietBatch(bitcask.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// ===================List数据结构=======================
// 定义一个单独的方法lpush和rpush都可以用到   参数中的isLeft是true的话就表示lpush   是false的话就表示rpush    返回元数据里的数据个数
func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	//查找对应的元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	//构造数据部分的key
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	//更新元数据和数据部分
	wb := rds.db.NewWrietBatch(bitcask.DefaultWriteBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	_ = wb.Put(key, meta.encode())
	_ = wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, nil
	}

	return meta.size, nil
}

func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

// 这里是pop的内在逻辑
func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	//查找对应的元数据
	meta, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}

	if meta.size == 0 {
		return nil, nil
	}

	//构造数据部分的key   注意这里与pushInner不一样
	lk := &listInternalKey{
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}

	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	//更新元数据
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}

	//注意一点,这里不需要writebatch是因为我们只需要进行meta数据的更新就可以了(因为我们不需要删除队列上的数,发证head和tail已经移动了,原来的位置上的数据已经废掉了)
	if err = rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}
	return element, nil
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

// ===================ZSet数据结构=======================
func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	//构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
	}

	//查看是否已经存在
	var exist = true
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		exist = false
	}
	if exist {
		//如果存在的话就对比score值,如果是一样的话就不需要进行任何操作
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	//更新元数据和数据
	wb := rds.db.NewWrietBatch(bitcask.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())

	}
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		_ = wb.Delete(oldKey.encodeWithScore())

	}

	//上述步骤处理完之后就更新真实的数据部分
	wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	wb.Put(zk.encodeWithScore(), nil) //不需要存值
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, err
}

// 根据用户传递的key和member,拿到最后的score
func (rds *RedisDataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}
	if meta.size == 0 {
		return -1, nil
	}

	//构造数据部分的key
	zk := &zsetInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}

func (rds *RedisDataStructure) findMetadata(key []byte, dataType redisDataType) (*metadata, error) {
	metabuf, err := rds.db.Get(key) //先根据key得到元数据的缓冲，其中元数据的结构：type+expire+version+size+(head+tail)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}
	var meta *metadata
	var exist = true
	if err == bitcask.ErrKeyNotFound { //这里表示元数据不存在，我们可以新建这样一个文件
		exist = false
	} else { //这里表示数据存在
		meta = decodeMetadata(metabuf) //将meta缓冲进行编码得到元数据
		//判断类型
		if meta.dataType != dataType { //获取到的数据和指定的数据类型不同
			return nil, ErrWrongTypeOperation
		}
		//判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() { //数据已经过期
			exist = false
		}
	}

	if !exist { //这里表示如果数据不存在或者已经过期，我们新建一个空的元数据即可
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(), //version字段是用来删除数据的
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
