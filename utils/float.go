package utils

import "strconv"

func FloatFromBytes(val []byte) float64 {
	f, _ := strconv.ParseFloat(string(val), 64)
	return f
}

func Float64ToBytes(val float64) []byte {
	//先转化为字符串,再转化为字节数组    'f'表示使用浮点数格式    -1表示不限制小数点位数    64表示精度
	return []byte(strconv.FormatFloat(val, 'f', -1, 64))
}
