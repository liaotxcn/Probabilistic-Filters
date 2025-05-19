package主干

import (
	"fmt"
	"math"
	"sync"

	"github.com/spaolacci/murmur3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GORM model定义
// BloomBit 布隆过滤器位图存储模型
// 使用MySQL表存储布隆过滤器的位图信息
type BloomBit struct {
	BitIndex uint64 `gorm:"primaryKey"`             // 位索引值(主键)，表示布隆过滤器中的位位置
	IsSet    bool   `gorm:"not null;default:false"` // 位状态标志，true表示该位已被设置
}

// MySQLBloomFilter MySQL实现的布隆过滤器结构体
// 使用MySQL作为后端存储，提供线程安全的布隆过滤器操作
type MySQLBloomFilter struct {
	db          *gorm.DB   // GORM数据库连接实例
	tableName   string     // MySQL表名，用于存储位图信息
	size        uint64     // 布隆过滤器位图总大小(位数)
	numHashFunc int        // 使用的哈希函数数量
	lock        sync.Mutex // 互斥锁，保证并发安全
}

// NewMySQLBloomFilter 初始化MySQL布隆过滤器
// 参数说明:
//
//	dsn: MySQL连接字符串，格式为"user:password@tcp(host:port)/dbname"
//	tableName: 存储位图的表名
//	expectedElements: 预期存储的元素数量
//	falsePositiveRate: 期望的误判率(0-1之间的小数)
//
// 返回值:
//
//	*MySQLBloomFilter: 初始化完成的布隆过滤器实例
//	error: 初始化过程中出现的错误
func NewMySQLBloomFilter(dsn, tableName string, expectedElements uint64, falsePositiveRate float64) (*MySQLBloomFilter, error) {
	// 根据预期元素数量和误判率计算最优参数
	size := optimalSize1(expectedElements, falsePositiveRate) // 计算最优位图大小
	numHashFunc := optimalNumHashFunc1(falsePositiveRate)     // 计算最优哈希函数数量

	// 初始化GORM数据库连接
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		PrepareStmt:            true, // 启用预编译语句，提高SQL执行效率
		SkipDefaultTransaction: true, // 跳过默认事务，提高批量操作性能
	})
	if err != nil {
		return nil, fmt.Errorf("数据库连接失败: %w", err)
	}

	// 自动迁移表结构
	err = db.Table(tableName).AutoMigrate(&BloomBit{})
	if err != nil {
		return nil, fmt.Errorf("表迁移失败: %w", err)
	}

	return &MySQLBloomFilter{
		db:          db,
		tableName:   tableName,
		size:        size,
		numHashFunc: numHashFunc,
	}, nil
}

// Add 向布隆过滤器添加单个元素
// 实现原理:
// 1. 对输入数据计算多个哈希值(位位置)
// 2. 将这些位位置标记为已设置(1)
// 3. 使用事务保证操作的原子性
// 参数:
//
//	data: 要添加的元素字节表示
//
// 返回值:
//
//	error: 添加过程中出现的错误
func (m *MySQLBloomFilter) Add(data []byte) error {
	m.lock.Lock()         // 获取互斥锁，保证线程安全
	defer m.lock.Unlock() // 确保锁会被释放

	// 计算元素的所有哈希位置
	hashes := m.getHashes(data)

	// 准备批量插入数据
	batch := make([]BloomBit, len(hashes))
	for i, hash := range hashes {
		batch[i] = BloomBit{
			BitIndex: hash, // 位索引
			IsSet:    true, // 设置为1
		}
	}

	// 使用事务执行批量插入
	// 如果位已存在则更新为1(ON CONFLICT DO UPDATE)
	return m.db.Table(m.tableName).Clauses(
		clause.OnConflict{
			Columns:   []clause.Column{{Name: "bit_index"}},                       // 冲突检测列
			DoUpdates: clause.Assignments(map[string]interface{}{"is_set": true}), // 冲突时更新
		},
	).Create(&batch).Error
}

// 批量添加元素到布隆过滤器
func (m *MySQLBloomFilter) BatchAdd(items [][]byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	// 预分配所有哈希的空间
	var totalHashes int
	for range items {
		totalHashes += m.numHashFunc
	}

	batch := make([]BloomBit, 0, totalHashes)
	for _, item := range items {
		hashes := m.getHashes(item)
		for _, hash := range hashes {
			batch = append(batch, BloomBit{BitIndex: hash, IsSet: true})
		}
	}

	// 批量插入提高性能
	return m.db.Table(m.tableName).Clauses(
		clause.OnConflict{
			Columns:   []clause.Column{{Name: "bit_index"}},
			DoUpdates: clause.Assignments(map[string]interface{}{"is_set": true}),
		},
	).CreateInBatches(&batch, 1000).Error
}

// 检查元素是否可能在布隆过滤器中
func (m *MySQLBloomFilter) Contains(data []byte) (bool, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	hashes := m.getHashes(data)
	var count int64

	// 在单个查询中检查所有位以提高性能
	err := m.db.Table(m.tableName).
		Where("bit_index IN ? AND is_set = true", hashes).
		Count(&count).Error

	if err != nil {
		return false, err
	}

	return count == int64(len(hashes)), nil
}

// 计算给定数据的哈希位置
func (m *MySQLBloomFilter) getHashes(data []byte) []uint64 {
	h1, h2 := murmur3.Sum128(data)
	hashes := make([]uint64, m.numHashFunc)
	for i := range hashes {
		hashes[i] = (h1 + uint64(i)*h2) % m.size
	}
	return hashes
}

// 计算最优位数组大小
func optimalSize1(n uint64, p float64) uint64 {
	return uint64(-float64(n) * math.Log(p) / (math.Log(2) * math.Log(2)))
}

// 计算最优哈希函数数量
func optimalNumHashFunc1(p float64) int {
	return int(-math.Log2(p))
}

// 关闭数据库连接
func (m *MySQLBloomFilter) Close() error {
	sqlDB, err := m.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// 重置布隆过滤器
func (m *MySQLBloomFilter) Clear() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.db.Table(m.tableName).Where("1 = 1").Delete(&BloomBit{}).Error
}

// 示例
func main() {
	// 初始化MySQL布隆过滤器
	dsn := "root:123456@tcp(127.0.0.1:3306)/bloom?charset=utf8&parseTime=True"
	bf, err := NewMySQLBloomFilter(dsn, "user_bloom_filter", 1000000, 0.01)
	if err != nil {
		panic(err)
	}
	defer bf.Close()

	// 添加单个元素
	if err := bf.Add([]byte("user1@example.com")); err != nil {
		panic(err)
	}

	// 批量添加元素
	items := [][]byte{
		[]byte("user2@example.com"),
		[]byte("user3@example.com"),
	}
	if err := bf.BatchAdd(items); err != nil {
		panic(err)
	}

	// 检查元素是否存在
	exists, err := bf.Contains([]byte("user1@example.com"))
	if err != nil {
		panic(err)
	}
	fmt.Println("user1 exists:", exists)

	// 检查不存在的元素
	exists, err = bf.Contains([]byte("nonexistent@example.com"))
	if err != nil {
		panic(err)
	}
	fmt.Println("nonexistent exists:", exists)
}
