package main

import (
	"encoding/binary"
	"fmt"
	"hash"
	"math"
	"sync"
	"sync/atomic"

	"github.com/spaolacci/murmur3" // 高性能非加密哈希函数库
)

// BloomFilter 表示一个线程安全的布隆过滤器数据结构
// 使用uint64数组存储位集合，支持并发读写操作
type BloomFilter struct {
	bitset      []uint64      // 位集合，每个uint64表示64位
	size        uint64        // 位集合的总位数
	hashFuncs   []hash.Hash64 // 哈希函数集合，用于元素映射
	numHashFunc int           // 使用的哈希函数数量
	lock        sync.RWMutex  // 读写锁，保证并发安全
	count       uint64        // 原子计数器，记录添加的元素数量
}

// NewBloomFilter 创建并初始化一个新的布隆过滤器
// 参数:
//
//	expectedElements: 预期存储的元素数量
//	falsePositiveRate: 期望的误判率(0-1之间的小数)
//
// 返回值:
//
//	初始化完成的布隆过滤器指针
func NewBloomFilter(expectedElements uint64, falsePositiveRate float64) *BloomFilter {
	// 计算最优的位集合大小和哈希函数数量
	size := optimalSize(expectedElements, falsePositiveRate)
	numHashFunc := optimalNumHashFunc(falsePositiveRate)

	// 初始化指定数量的哈希函数
	hashFuncs := make([]hash.Hash64, numHashFunc)
	for i := 0; i < numHashFunc; i++ {
		hashFuncs[i] = murmur3.New64() // 使用MurmurHash3算法
	}

	return &BloomFilter{
		bitset:      make([]uint64, (size+63)/64), // 按64位对齐分配内存
		size:        size,
		hashFuncs:   hashFuncs,
		numHashFunc: numHashFunc,
	}
}

// Add 向布隆过滤器中添加一个元素
// 参数:
//
//	data: 要添加的元素的字节表示
//	此方法是线程安全的，但会阻塞其他并发操作
func (bf *BloomFilter) Add(data []byte) {
	bf.lock.Lock()
	defer bf.lock.Unlock()

	// 使用所有哈希函数计算位位置并设置
	for i := 0; i < bf.numHashFunc; i++ {
		bf.hashFuncs[i].Reset()
		bf.hashFuncs[i].Write(data)
		hashValue := bf.hashFuncs[i].Sum64() % bf.size
		word, bit := hashValue/64, hashValue%64 // 计算uint64数组索引和位偏移
		bf.bitset[word] |= 1 << bit             // 设置对应位
	}
	atomic.AddUint64(&bf.count, 1) // 原子递增计数器
}

// Contains 检查元素是否可能存在于布隆过滤器中
// 参数:
//
//	data: 要查询的元素的字节表示
//
// 返回值:
//
//	true: 元素可能存在(可能有误判)
//	false: 元素绝对不存在(100%准确)
func (bf *BloomFilter) Contains(data []byte) bool {
	bf.lock.RLock()
	defer bf.lock.RUnlock()

	// 检查所有哈希函数对应的位是否都为1
	for i := 0; i < bf.numHashFunc; i++ {
		bf.hashFuncs[i].Reset()
		bf.hashFuncs[i].Write(data)
		hashValue := bf.hashFuncs[i].Sum64() % bf.size
		word, bit := hashValue/64, hashValue%64
		if bf.bitset[word]&(1<<bit) == 0 {
			return false // 任意一位为0即可确定不存在
		}
	}
	return true // 所有位都为1，元素可能存在
}

// Count 估算元素数量
func (bf *BloomFilter) Count() uint64 {
	return atomic.LoadUint64(&bf.count)
}

// FalsePositiveRate 计算当前误判率
func (bf *BloomFilter) FalsePositiveRate() float64 {
	bf.lock.RLock()
	defer bf.lock.RUnlock()

	setBits := 0
	for _, bit := range bf.bitset {
		if bit != 0 {
			setBits++
		}
	}
	return math.Pow(1-math.Exp(-float64(bf.numHashFunc)*float64(setBits)/float64(bf.size)), float64(bf.numHashFunc))
}

func main() {
	// 示例用法
	bf := NewBloomFilter(1000000, 0.01) // 预期100万元素，1%误判率

	// 添加元素
	bf.Add([]byte("hello"))
	bf.Add([]byte("world"))

	// 检查元素
	println(bf.Contains([]byte("hello"))) // true
	println(bf.Contains([]byte("world"))) // true
	println(bf.Contains([]byte("foo")))   // false (可能)
	println(bf.Contains([]byte("hel")))   // false (可能)
	println(bf.Contains([]byte("abcde"))) // false (可能)

	// 获取当前误判率
	println("Current false positive rate:", bf.FalsePositiveRate())
}

// optimalSize 计算最优的位集合大小
// 基于公式: m = -n*ln(p)/(ln2)^2
// 其中n是预期元素数量，p是期望误判率
func optimalSize(n uint64, p float64) uint64 {
	return uint64(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2)))
}

// optimalNumHashFunc 计算最优的哈希函数数量
// 基于公式: k = -log2(p)
func optimalNumHashFunc(p float64) int {
	return int(-math.Log2(p))
}

// 添加并行处理支持
func (bf *BloomFilter) AddParallel(data [][]byte) {
	var wg sync.WaitGroup
	for _, d := range data {
		wg.Add(1)
		go func(d []byte) {
			defer wg.Done()
			bf.Add(d)
		}(d)
	}
	wg.Wait()
}

// 修改哈希计算方式
// 优化后的哈希计算方法
func (bf *BloomFilter) getHashes(data []byte) []uint64 {
	h1, h2 := murmur3.Sum128(data)
	hashes := make([]uint64, bf.numHashFunc)
	for i := range hashes {
		hashes[i] = (h1 + uint64(i)*h2) % bf.size
	}
	return hashes
}

// 完整的序列化实现
func (bf *BloomFilter) Serialize() ([]byte, error) {
	bf.lock.RLock()
	defer bf.lock.RUnlock()

	data := make([]byte, 8+8+4+len(bf.bitset)*8)
	binary.LittleEndian.PutUint64(data[0:8], bf.size)
	binary.LittleEndian.PutUint64(data[8:16], uint64(bf.numHashFunc))
	binary.LittleEndian.PutUint32(data[16:20], uint32(bf.count))

	for i, bits := range bf.bitset {
		binary.LittleEndian.PutUint64(data[20+i*8:28+i*8], bits)
	}

	return data, nil
}

// 完整的反序列化实现
func Deserialize(data []byte) (*BloomFilter, error) {
	if len(data) < 20 {
		return nil, fmt.Errorf("invalid data length")
	}

	size := binary.LittleEndian.Uint64(data[0:8])
	numHashFunc := int(binary.LittleEndian.Uint64(data[8:16]))
	count := binary.LittleEndian.Uint32(data[16:20])

	bitsetSize := (len(data) - 20) / 8
	bitset := make([]uint64, bitsetSize)
	for i := 0; i < bitsetSize; i++ {
		bitset[i] = binary.LittleEndian.Uint64(data[20+i*8 : 28+i*8])
	}

	bf := &BloomFilter{
		bitset:      bitset,
		size:        size,
		numHashFunc: numHashFunc,
		count:       uint64(count),
		hashFuncs:   make([]hash.Hash64, numHashFunc),
	}

	for i := 0; i < numHashFunc; i++ {
		bf.hashFuncs[i] = murmur3.New64()
	}

	return bf, nil
}

// 优化后的并行处理方法
func (bf *BloomFilter) AddParallelWithWorkers(data [][]byte, workers int) {
	var wg sync.WaitGroup
	ch := make(chan []byte, len(data))

	for _, d := range data {
		ch <- d
	}
	close(ch)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range ch {
				bf.Add(d)
			}
		}()
	}
	wg.Wait()
}
