package主干

import (
	"BloomFilter/Cuckoo"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisCuckooFilter 结合Redis和内存布谷鸟过滤器的实现
// 特点：
// 1. 使用Redis作为持久化存储
// 2. 内存中使用布谷鸟过滤器提高查询性能
// 3. 读写操作保证线程安全
type RedisCuckooFilter struct {
	filter    *Cuckoo.CuckooFilter // 内存布谷鸟过滤器实例
	redisCli  *redis.Client        // Redis客户端
	keyPrefix string               // Redis键前缀
	mu        sync.RWMutex         // 读写锁，保证并发安全
	stats     FilterStats          // 过滤器统计信息
}

// FilterStats 记录过滤器的操作统计
// 用于监控和性能分析
type FilterStats struct {
	InsertCount    int64     // 插入操作次数
	DeleteCount    int64     // 删除操作次数
	LookupCount    int64     // 查询操作次数
	FalsePositive  int64     // 误报次数统计
	LastExpandTime time.Time // 最后一次扩容时间
}

// NewRedisCuckooFilter 创建并初始化Redis布谷鸟过滤器
// 参数：
//   - capacity: 过滤器初始容量
//   - falsePositiveRate: 期望的误报率(0-1之间)
//   - redisOpts: Redis连接配置
//   - keyPrefix: Redis键前缀
//
// 返回值：
//   - 初始化好的RedisCuckooFilter实例
func NewRedisCuckooFilter(capacity uint, falsePositiveRate float64, redisOpts *redis.Options, keyPrefix string) *RedisCuckooFilter {
	// 初始化Redis客户端
	rdb := redis.NewClient(redisOpts)

	// 优化Redis连接池配置
	rdb.Options().PoolSize = 20                 // 最大连接数
	rdb.Options().MinIdleConns = 5              // 最小空闲连接数
	rdb.Options().IdleTimeout = 5 * time.Minute // 空闲连接超时时间

	// 初始化内存布谷鸟过滤器
	cf := Cuckoo.NewCuckooFilter(int(capacity), falsePositiveRate)

	return &RedisCuckooFilter{
		filter:    cf,
		redisCli:  rdb,
		keyPrefix: keyPrefix,
	}
}

// checkAndExpand 检查并扩展过滤器容量
func (rcf *RedisCuckooFilter) checkAndExpand() {
	rcf.mu.Lock()
	defer rcf.mu.Unlock()

	// 当使用率达到80%时自动扩容
	if float64(rcf.filter.Count())/float64(rcf.filter.Capacity()) > 0.8 {
		newCapacity := rcf.filter.Capacity() * 2
		newFilter := Cuckoo.NewCuckooFilter(newCapacity, rcf.filter.FalsePositiveRate())

		// 重新加载所有数据
		rcf.loadAllToFilter(newFilter)

		rcf.filter = newFilter
		rcf.stats.LastExpandTime = time.Now()
		log.Printf("过滤器已扩容至 %d", newCapacity)
	}
}

// loadAllToFilter 从Redis加载所有数据到指定过滤器
func (rcf *RedisCuckooFilter) loadAllToFilter(filter *Cuckoo.CuckooFilter) {
	ctx := context.Background()
	keys, _ := rcf.redisCli.Keys(ctx, rcf.keyPrefix+"*").Result()

	for _, key := range keys {
		val, err := rcf.redisCli.Get(ctx, key).Bytes()
		if err == nil {
			filter.Insert(val)
		}
	}
}

// LoadFromRedis 从Redis加载数据到内存过滤器
func (rcf *RedisCuckooFilter) LoadFromRedis(ctx context.Context) error {
	rcf.mu.Lock()
	defer rcf.mu.Unlock()

	// 获取Redis中所有键
	keys, err := rcf.redisCli.Keys(ctx, rcf.keyPrefix+"*").Result()
	if err != nil {
		return fmt.Errorf("获取Redis键失败: %w", err)
	}

	// 加载每个键对应的值到过滤器
	for _, key := range keys {
		val, err := rcf.redisCli.Get(ctx, key).Bytes()
		if err != nil {
			log.Printf("获取键 %s 的值失败: %v", key, err)
			continue
		}

		if !rcf.filter.Insert(val) {
			log.Printf("插入值到过滤器失败: %s", key)
		}
	}

	log.Printf("从Redis成功加载 %d 个元素到过滤器", len(keys))
	return nil
}

// Insert 插入元素到过滤器
// 先写入Redis保证持久化，再写入内存过滤器提高查询性能
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - item: 要插入的元素([]byte格式)
//
// 返回值：
//   - error: 操作错误信息
func (rcf *RedisCuckooFilter) Insert(ctx context.Context, item []byte) error {
	rcf.mu.Lock()         // 获取写锁
	defer rcf.mu.Unlock() // 确保锁释放

	// 1. 先持久化到Redis
	key := rcf.keyPrefix + string(item)
	if err := rcf.redisCli.Set(ctx, key, item, 0).Err(); err != nil {
		return fmt.Errorf("Redis插入失败: %w", err)
	}

	// 2. 再插入到内存过滤器
	if !rcf.filter.Insert(item) {
		return fmt.Errorf("过滤器插入失败，可能已达容量上限")
	}

	// 更新统计信息
	rcf.stats.InsertCount++
	return nil
}

// Lookup 查询元素是否存在
// 仅查询内存过滤器，不访问Redis
// 参数：
//   - item: 要查询的元素([]byte格式)
//
// 返回值：
//   - bool: 元素是否存在(可能有误报)
func (rcf *RedisCuckooFilter) Lookup(item []byte) bool {
	rcf.mu.RLock()         // 获取读锁
	defer rcf.mu.RUnlock() // 确保锁释放

	// 更新统计信息
	rcf.stats.LookupCount++

	// 查询内存过滤器
	exists := rcf.filter.Lookup(item)
	if exists {
		rcf.stats.FalsePositive++ // 记录可能的误报
	}
	return exists
}

// Delete 删除元素
// 先删除Redis中的数据，再删除内存过滤器中的对应项
// 参数：
//   - ctx: 上下文，用于控制超时和取消
//   - item: 要删除的元素([]byte格式)
//
// 返回值：
//   - error: 操作错误信息
func (rcf *RedisCuckooFilter) Delete(ctx context.Context, item []byte) error {
	rcf.mu.Lock()         // 获取写锁
	defer rcf.mu.Unlock() // 确保锁释放

	// 1. 从Redis删除
	key := rcf.keyPrefix + string(item)
	if err := rcf.redisCli.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("Redis删除失败: %w", err)
	}

	// 2. 从内存过滤器删除
	rcf.filter.Delete(item)

	// 更新统计信息
	rcf.stats.DeleteCount++
	return nil
}

// SyncToRedis 定期同步内存过滤器到Redis
func (rcf *RedisCuckooFilter) SyncToRedis(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rcf.mu.Lock()
			// 这里可以实现增量同步逻辑
			rcf.mu.Unlock()
		}
	}
}

func main() {
	// Redis连接配置
	redisOpts := &redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 无密码
		DB:       0,  // 默认DB
	}

	// 创建Redis布谷鸟过滤器
	rcf := NewRedisCuckooFilter(100000, 0.03, redisOpts, "cuckoo:")

	ctx := context.Background()

	// 从Redis加载数据
	if err := rcf.LoadFromRedis(ctx); err != nil {
		log.Fatal(err)
	}

	// 启动定期同步
	go rcf.SyncToRedis(ctx, 5*time.Minute)

	// 使用示例
	item := []byte("test_item")
	if err := rcf.Insert(ctx, item); err != nil {
		log.Printf("插入失败: %v", err)
	}

	if rcf.Lookup(item) {
		fmt.Println("元素存在")
	}

	if err := rcf.Delete(ctx, item); err != nil {
		log.Printf("删除失败: %v", err)
	}
}
