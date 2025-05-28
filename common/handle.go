package common

import (
	"bytes"
	"sync"
)

// 改为池化指向切片的指针

var bytePool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 1024) // 初始容量
		return &buf                  // 返回指针
	},
}

func InitBytePool(size int) {
	bytePool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 0, size*1024) // 初始容量
			return &buf                       // 返回指针
		},
	}
}

func ConcatBytesOptimized(values []string, sep string) string {
	// 处理边界情况
	switch len(values) {
	case 0:
		return ""
	case 1:
		return values[0]
	}

	// 计算总长度（含分隔符）
	total := 0
	sepLen := len(sep)
	for _, s := range values {
		total += len(s)
	}
	total += sepLen * (len(values) - 1) // 分隔符总长度

	// 获取/创建缓冲区 - 正确处理指针
	bufPtr := bytePool.Get().(*[]byte)
	buf := *bufPtr // 解引用获取实际切片

	if cap(buf) < total {
		// 如果容量不够，创建新切片
		buf = make([]byte, 0, total)
		*bufPtr = buf // 更新指针指向的切片
	} else {
		// 重用现有容量
		buf = buf[:0]
	}

	// 带分隔符的拼接逻辑
	buf = append(buf, values[0]...)
	for _, s := range values[1:] {
		buf = append(buf, sep...)
		buf = append(buf, s...)
	}

	// 返回结果并正确回收缓冲区
	result := string(buf)
	*bufPtr = buf[:0]    // 更新指针指向的切片，重置长度
	bytePool.Put(bufPtr) // 放回指针对象
	return result
}

// 使用bytes.Buffer替代字符串拼接 - 保留但不推荐使用的实现
func ConcatBytesOptimized1(parts []string, separator string) string {
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}

	// 预估总长度以减少分配
	totalLen := 0
	for _, s := range parts {
		totalLen += len(s)
	}
	totalLen += len(separator) * (len(parts) - 1)

	// 使用预分配的buffer
	var buf bytes.Buffer
	buf.Grow(totalLen)

	buf.WriteString(parts[0])
	for _, s := range parts[1:] {
		buf.WriteString(separator)
		buf.WriteString(s)
	}

	return buf.String()
}
