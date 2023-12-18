package redis

import "errors"

//实现redis中几种通用的命令，不用管数据是String、Hash或者别的

func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDataStructure) Type(key []byte) (redisDataType, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, errors.New("value is null")
	}
	//第一个字节就是类型
	return encValue[0], nil
}
