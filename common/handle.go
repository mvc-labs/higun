package common

import (
	"bytes"
	"sync"
)

// Change to pool pointers to slices

var bytePool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 1024) // Initial capacity
		return &buf                  // Return pointer
	},
}

func InitBytePool(size int) {
	bytePool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 0, size*1024) // Initial capacity
			return &buf                       // Return pointer
		},
	}
}

func ConcatBytesOptimized(values []string, sep string) string {
	// Handle edge cases
	switch len(values) {
	case 0:
		return ""
	case 1:
		return values[0]
	}

	// Calculate total length (including separators)
	total := 0
	sepLen := len(sep)
	for _, s := range values {
		total += len(s)
	}
	total += sepLen * (len(values) - 1) // Total separator length

	// Get/create buffer - correctly handle pointer
	bufPtr := bytePool.Get().(*[]byte)
	buf := *bufPtr // Dereference to get actual slice

	if cap(buf) < total {
		// If capacity is insufficient, create new slice
		buf = make([]byte, 0, total)
		*bufPtr = buf // Update the slice pointed to by the pointer
	} else {
		// Reuse existing capacity
		buf = buf[:0]
	}

	// Concatenation logic with separators
	buf = append(buf, values[0]...)
	for _, s := range values[1:] {
		buf = append(buf, sep...)
		buf = append(buf, s...)
	}

	// Return result and correctly recycle buffer
	result := string(buf)
	*bufPtr = buf[:0]    // Update the slice pointed to by the pointer, reset length
	bytePool.Put(bufPtr) // Put back the pointer object
	return result
}

// Use bytes.Buffer instead of string concatenation - keep but not recommended implementation
func ConcatBytesOptimized1(parts []string, separator string) string {
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}

	// Estimate total length to reduce allocations
	totalLen := 0
	for _, s := range parts {
		totalLen += len(s)
	}
	totalLen += len(separator) * (len(parts) - 1)

	// Use pre-allocated buffer
	var buf bytes.Buffer
	buf.Grow(totalLen)

	buf.WriteString(parts[0])
	for _, s := range parts[1:] {
		buf.WriteString(separator)
		buf.WriteString(s)
	}

	return buf.String()
}
