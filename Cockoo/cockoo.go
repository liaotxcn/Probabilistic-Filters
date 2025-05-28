package Cuckoo

import (
	"fmt"
	"hash/fnv"
	"math"
	"runtime"
	"sync"
	"time"
)

type CuckooFilter struct {
	buckets    [][]uint32
	bucketSize int
	maxKicks   int
	fingerLen  int
	count      int
	mu         sync.RWMutex // 添加读写锁
}

// FalsePositiveRate 返回当前过滤器的误报率
func (cf *CuckooFilter) FalsePositiveRate() float64 {
	cf.mu.RLock()
	defer cf.mu.RUnlock()
	// 误报率计算公式: (2b)/2^fingerLen
	// 其中b是每个桶的条目数
	return float64(2*cf.bucketSize) / math.Pow(2, float64(cf.fingerLen))
}

// Capacity 返回过滤器的总容量(桶数量 * 每个桶的条目数)
func (cf *CuckooFilter) Capacity() int {
	cf.mu.RLock()
	defer cf.mu.RUnlock()
	return len(cf.buckets) * cf.bucketSize
}

// NewCuckooFilter 初始化一个新的布谷鸟过滤器
// capacity: 预期存储的元素数量
// falsePositiveRate: 期望的误报率(0-1之间)
func NewCuckooFilter(capacity int, falsePositiveRate float64) *CuckooFilter {
	// 计算指纹长度，基于误报率和每个桶的条目数
	// fingerprint_size = ceil(log2(2b/ε))
	// 其中b是每个桶的条目数，ε是误报率
	bucketSize := 4 // 每个桶固定4个条目
	fingerLen := int(math.Ceil(math.Log2(2 * float64(bucketSize) / falsePositiveRate)))

	// 计算桶数量，增加5%缓冲防止过早填满
	numBuckets := int(math.Ceil(float64(capacity) / float64(bucketSize) * 1.05))

	// 初始化二维数组表示桶集合
	buckets := make([][]uint32, numBuckets)
	for i := range buckets {
		buckets[i] = make([]uint32, bucketSize) // 每个桶初始化为全0
	}

	return &CuckooFilter{
		buckets:    buckets,
		bucketSize: bucketSize,
		maxKicks:   500,       // 最大踢出次数(防止无限循环)
		fingerLen:  fingerLen, // 指纹bit长度
		count:      0,         // 当前元素计数
	}
}

// hash 计算元素的哈希值和指纹
// 返回: (哈希值, 指纹)
func (cf *CuckooFilter) hash(item []byte) (uint32, uint32) {
	h := fnv.New32a()
	h.Write(item)
	// 通过掩码获取指纹的低fingerLen位
	fingerprint := h.Sum32() & ((1 << cf.fingerLen) - 1)
	if fingerprint == 0 {
		fingerprint = 1 // 确保指纹不为0(0表示空槽位)
	}

	// 计算第二个哈希值(通过加盐)
	h.Reset()
	h.Write(item)
	h.Write([]byte("salt")) // 加盐确保与第一个哈希不同
	hash := h.Sum32()
	return hash, fingerprint
}

// Insert 插入元素到过滤器
// 返回: 是否插入成功
func (cf *CuckooFilter) Insert(item []byte) bool {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	h1, fingerprint := cf.hash(item)
	i1 := cf.getIndex(h1) // 第一个候选桶位置

	// 尝试插入第一个候选桶
	if cf.insertToBucket(i1, fingerprint) {
		cf.count++
		return true
	}

	// 计算第二个候选桶位置: i2 = i1 xor hash(fingerprint)
	// 这是布谷鸟哈希的关键特性，两个位置可以互相推导
	i2 := cf.getIndex(h1 ^ cf.getIndex(fingerprint))

	// 尝试插入第二个候选桶
	if cf.insertToBucket(i2, fingerprint) {
		cf.count++
		return true
	}

	// 两个桶都满了，执行踢出操作
	return cf.reinsert(i1, i2, fingerprint)
}

// reinsert 重新安置元素(核心踢出算法)
// 随机选择一个桶中的元素踢出，尝试插入到它的备用位置
func (cf *CuckooFilter) reinsert(i1, i2 uint32, fingerprint uint32) bool {
	for k := 0; k < cf.maxKicks; k++ {
		// 交替选择两个桶
		var index uint32
		if k%2 == 0 {
			index = i1
		} else {
			index = i2
		}

		// 随机选择桶中的一个槽位
		j := k % cf.bucketSize
		oldFp := cf.buckets[index][j]

		// 踢出旧指纹，放入新指纹
		cf.buckets[index][j] = fingerprint
		fingerprint = oldFp

		// 计算被踢出指纹的备用位置
		newIndex := index ^ cf.getIndex(fingerprint)
		if cf.insertToBucket(newIndex, fingerprint) {
			cf.count++
			return true
		}
	}
	return false // 超过最大踢出次数仍未找到空位
}

func (cf *CuckooFilter) getIndex(hash uint32) uint32 {
	return hash % uint32(len(cf.buckets))
}

func (cf *CuckooFilter) insertToBucket(index uint32, fingerprint uint32) bool {
	for i, val := range cf.buckets[index] {
		if val == 0 {
			cf.buckets[index][i] = fingerprint
			return true
		}
	}
	return false
}

// Lookup 检查元素是否可能存在于过滤器中
// 注意：可能存在误报(返回true但实际不存在)，但不会漏报(返回false则一定不存在)
func (cf *CuckooFilter) Lookup(item []byte) bool {
	cf.mu.RLock() // 读锁保护并发安全
	defer cf.mu.RUnlock()

	h1, fingerprint := cf.hash(item)
	i1 := cf.getIndex(h1)

	// 检查第一个候选桶
	if cf.containsInBucket(i1, fingerprint) {
		return true
	}

	// 检查第二个候选桶
	h2 := h1 ^ cf.getIndex(fingerprint) // 使用异或计算备用位置
	i2 := cf.getIndex(h2)
	return cf.containsInBucket(i2, fingerprint)
}

// containsInBucket 检查指纹是否存在于指定桶中
func (cf *CuckooFilter) containsInBucket(index uint32, fingerprint uint32) bool {
	// 线性扫描桶中的每个槽位
	for _, val := range cf.buckets[index] {
		if val == fingerprint {
			return true
		}
	}
	return false
}

// Delete 从过滤器中删除元素
// 返回: 是否成功删除(元素不存在时返回false)
func (cf *CuckooFilter) Delete(item []byte) bool {
	cf.mu.Lock() // 写锁保护并发安全
	defer cf.mu.Unlock()

	h1, fingerprint := cf.hash(item)
	i1 := cf.getIndex(h1)

	// 尝试从第一个桶删除
	if cf.deleteFromBucket(i1, fingerprint) {
		cf.count--
		return true
	}

	// 尝试从第二个桶删除
	i2 := cf.getIndex(h1 ^ cf.getIndex(fingerprint))
	if cf.deleteFromBucket(i2, fingerprint) {
		cf.count--
		return true
	}
	return false
}

// deleteFromBucket 从指定桶中删除指纹
func (cf *CuckooFilter) deleteFromBucket(index uint32, fingerprint uint32) bool {
	for i, val := range cf.buckets[index] {
		if val == fingerprint {
			cf.buckets[index][i] = 0 // 置0表示空槽位
			return true
		}
	}
	return false
}

// Count 返回过滤器中当前元素数量
// 注意：由于并发操作，返回的值可能不是精确的
func (cf *CuckooFilter) Count() int {
	cf.mu.RLock()
	defer cf.mu.RUnlock()
	return cf.count
}

// BatchInsert 批量插入元素(线程安全)
// 返回: 成功插入的元素数量
func (cf *CuckooFilter) BatchInsert(items [][]byte) int {
	cf.mu.Lock()
	defer cf.mu.Unlock()

	success := 0
	for _, item := range items {
		if cf.insert(item) { // 使用内部非线程安全方法提高性能
			success++
		}
	}
	return success
}

// 内部非并发安全插入方法
func (cf *CuckooFilter) insert(item []byte) bool {
	h1, fingerprint := cf.hash(item)
	i1 := cf.getIndex(h1)

	// 尝试插入第一个桶
	if cf.insertToBucket(i1, fingerprint) {
		cf.count++
		return true
	}

	// 尝试插入第二个桶
	h2 := h1 ^ cf.getIndex(fingerprint)
	i2 := cf.getIndex(h2)
	if cf.insertToBucket(i2, fingerprint) {
		cf.count++
		return true
	}

	// 需要重新安置
	return cf.reinsert(i1, i2, fingerprint)
}

func cuckoo() {
	// 基础功能测试
	testBasicOperations()

	// 并发测试
	testConcurrentOperations()

	// 性能测试
	testPerformance()
}

// testBasicOperations 测试过滤器的基本功能
// 包括插入、查找、删除和误报率测试
func testBasicOperations() {
	fmt.Println("=== 基础功能测试 ===")
	filter := NewCuckooFilter(1000, 0.01) // 创建测试过滤器

	// 测试数据准备
	items := [][]byte{[]byte("apple"), []byte("banana"), []byte("orange")}

	// 测试插入和查找功能
	for _, item := range items {
		filter.Insert(item)          // 插入测试数据
		found := filter.Lookup(item) // 验证数据存在
		fmt.Printf("插入并查找 %s: %v\n", item, found)
	}

	// 测试误报率 - 查找不存在的数据
	nonExistItem := []byte("pear")
	falsePositive := filter.Lookup(nonExistItem)
	fmt.Printf("误报测试(pear): %v (期望false)\n", falsePositive)

	// 测试删除功能
	deleted := filter.Delete(items[0])
	fmt.Printf("删除 %s: %v (期望true)\n", items[0], deleted)

	// 验证删除后查找应返回false
	foundAfterDelete := filter.Lookup(items[0])
	fmt.Printf("删除后查找 %s: %v (期望false)\n", items[0], foundAfterDelete)
}

// testConcurrentOperations 测试并发场景下的线程安全性
// 模拟高并发插入和查找操作
func testConcurrentOperations() {
	fmt.Println("\n=== 并发测试 ===")
	filter := NewCuckooFilter(10000, 0.01) // 创建大容量过滤器
	var wg sync.WaitGroup                  // 用于等待所有goroutine完成

	// 并发插入测试
	start := time.Now()
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", id)
			filter.Insert([]byte(key)) // 并发插入
		}(i)
	}
	wg.Wait() // 等待所有插入完成

	// 并发查找测试
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", id)
			filter.Lookup([]byte(key)) // 并发查找
		}(i)
	}
	wg.Wait()

	fmt.Printf("并发操作完成, 耗时: %v\n", time.Since(start))
}

// testPerformance 测试过滤器性能指标
// 包括插入速度、查找速度和内存占用
func testPerformance() {
	fmt.Println("\n=== 性能测试 ===")
	filter := NewCuckooFilter(100000, 0.01) // 创建大容量过滤器

	// 测试插入性能
	start := time.Now()
	for i := 0; i < 100000; i++ {
		filter.Insert([]byte(fmt.Sprintf("item%d", i))) // 批量插入
	}
	fmt.Printf("插入100000个项目耗时: %v\n", time.Since(start))

	// 测试查找性能
	start = time.Now()
	for i := 0; i < 100000; i++ {
		filter.Lookup([]byte(fmt.Sprintf("item%d", i))) // 批量查找
	}
	fmt.Printf("查找100000个项目耗时: %v\n", time.Since(start))

	// 测试内存占用
	var m runtime.MemStats
	runtime.ReadMemStats(&m) // 获取内存统计
	fmt.Printf("内存使用: %.2f MB\n", float64(m.Alloc)/1024/1024)
}
