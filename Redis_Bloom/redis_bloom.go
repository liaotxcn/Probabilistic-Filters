package主干

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/spaolacci/murmur3"
)

// 使用Redis位图(bitmap)功能实现高效的概率型数据结构
type RedisBloomFilter struct {
	client      *redis.Client // Redis客户端实例
	key         string        // Redis中存储位图键名
	numHashFunc int           // 哈希函数数量
	size        uint64        // 位图总大小(位数)
	ttl         time.Duration // 键过期时间(0表永不过期)
}

// 创建新的Redis布隆过滤器实例
// 参数说明:
//
//	client: 已初始化的Redis客户端
//	key: Redis键名，用于存储位图数据
//	expectedElements: 预期存储的元素数量
//	falsePositiveRate: 期望的误判率(0-1之间的小数)
//	ttl: 键的过期时间，0表示永不过期
//
// 返回值:
//
// 初始化完成的布隆过滤器实例
func NewRedisBloomFilter(client *redis.Client, key string, expectedElements uint64, falsePositiveRate float64, ttl time.Duration) *RedisBloomFilter {
	// 根据预期元素数量和误判率计算最优参数
	size := optimalSize2(expectedElements, falsePositiveRate) // 计算最优位图大小
	numHashFunc := optimalNumHashFunc2(falsePositiveRate)     // 计算最优哈希函数数量

	return &RedisBloomFilter{
		client:      client,
		key:         key,
		numHashFunc: numHashFunc,
		size:        size,
		ttl:         ttl,
	}
}

// 向布隆过滤器添加单个元素
// 实现原理:
// 1. 使用多个哈希函数计算元素的位位置
// 2. 在Redis位图中将这些位置设为1
// 3. 使用管道(pipeline)批量操作提高性能
// 参数:
//
//	ctx: 上下文对象，用于控制请求超时等
//	data: 要添加的元素字节表示
//
// 返回值:
//
//	error: 操作过程中出现的错误
func (r *RedisBloomFilter) Add(ctx context.Context, data []byte) error {
	hashes := r.getHashes(data)
	pipe := r.client.Pipeline()

	for _, hash := range hashes {
		pipe.SetBit(ctx, r.key, int64(hash), 1)
	}

	// 设置TTL(若配置了过期时间)
	if r.ttl > 0 {
		pipe.Expire(ctx, r.key, r.ttl)
	}

	_, err := pipe.Exec(ctx)
	return err
}

// 检查元素是否可能存在于布隆过滤器中
// 注意: 可能存在误判(假阳性)，但不会漏判(假阴性)
// 实现原理:
// 1. 计算元素的所有哈希位置
// 2. 检查Redis位图中这些位置是否都为1
// 3. 如果所有位都为1则返回true，否则返回false
// 参数:
//
//	ctx: 上下文对象
//	data: 要检查的元素字节表示
//
// 返回值:
//
//	bool: 元素是否存在(可能有误判)
//	error: 操作过程中出现的错误
func (r *RedisBloomFilter) Contains(ctx context.Context, data []byte) (bool, error) {
	hashes := r.getHashes(data)
	pipe := r.client.Pipeline()

	cmds := make([]*redis.IntCmd, len(hashes))
	for i, hash := range hashes {
		cmds[i] = pipe.GetBit(ctx, r.key, int64(hash))
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}

	for _, cmd := range cmds {
		if cmd.Val() == 0 {
			return false, nil
		}
	}
	return true, nil
}

// 计算元素的多个哈希位置
// 使用双重哈希技术模拟多个哈希函数
// 参数:
//
//	data: 输入数据的字节表示
//
// 返回值:
//
//	[]uint64: 计算得到的哈希位置数组
func (r *RedisBloomFilter) getHashes(data []byte) []uint64 {
	h1, h2 := murmur3.Sum128(data)
	hashes := make([]uint64, r.numHashFunc)
	for i := range hashes {
		hashes[i] = (h1 + uint64(i)*h2) % r.size
	}
	return hashes
}

// 计算最优的位集合大小
func optimalSize2(n uint64, p float64) uint64 {
	return uint64(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2)))
}

// 计算最优的哈希函数数量
func optimalNumHashFunc2(p float64) int {
	return int(-math.Log2(p))
}

// 添加单个元素到布隆过滤器
func (r *RedisBloomFilter) Add2(ctx context.Context, data []byte) error {
	hashes := r.getHashes(data)
	pipe := r.client.Pipeline()

	for _, hash := range hashes {
		pipe.SetBit(ctx, r.key, int64(hash), 1)
	}

	// 设置TTL(如果配置了过期时间)
	if r.ttl > 0 {
		pipe.Expire(ctx, r.key, r.ttl)
	}

	_, err := pipe.Exec(ctx)
	return err
}

// 检查元素是否可能存在
func (r *RedisBloomFilter) Contains2(ctx context.Context, data []byte) (bool, error) {
	hashes := r.getHashes(data)
	pipe := r.client.Pipeline()

	cmds := make([]*redis.IntCmd, len(hashes))
	for i, hash := range hashes {
		cmds[i] = pipe.GetBit(ctx, r.key, int64(hash))
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}

	for _, cmd := range cmds {
		if cmd.Val() == 0 {
			return false, nil
		}
	}
	return true, nil
}

// 示例用法更新
func main() {
	// 初始化Redis客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Redis地址
		Password: "",               // 密码
		DB:       0,                // 数据库
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 创建布隆过滤器(添加TTL参数)
	bf := NewRedisBloomFilter(rdb, "user:bloomfilter", 1000000, 0.01, 24*time.Hour)

	// 添加元素
	if err := bf.Add(ctx, []byte("user1@example.com")); err != nil {
		panic(err)
	}
	if err := bf.Add(ctx, []byte("user2@example.com")); err != nil {
		panic(err)
	}

	// 检查元素是否存在
	exists, err := bf.Contains(ctx, []byte("user1@example.com"))
	if err != nil {
		panic(err)
	}
	fmt.Println("user1 exists:", exists)

	exists, err = bf.Contains(ctx, []byte("unknown@example.com"))
	if err != nil {
		panic(err)
	}
	fmt.Println("unknown exists:", exists)
}
