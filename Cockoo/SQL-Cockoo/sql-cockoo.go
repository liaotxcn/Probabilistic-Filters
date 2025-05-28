package主干

import (
	"BloomFilter/Cuckoo"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Config 配置参数
type Config struct {
	MySQLDSN          string  // MySQL连接字符串
	FilterCapacity    int     // 过滤器初始容量
	FalsePositiveRate float64 // 期望的误报率(0-1之间)
}

// UserBlacklist 用户黑名单过滤器结构体
type UserBlacklist struct {
	filter *Cuckoo.CuckooFilter // 布谷鸟过滤器实例
	db     *gorm.DB             // 数据库连接
	mu     sync.RWMutex         // 读写锁，保证并发安全
	config Config               // 配置参数
	stats  Stats                // 性能统计
}

// Stats 性能统计结构体
type Stats struct {
	InsertCount    int64     // 插入操作计数
	DeleteCount    int64     // 删除操作计数
	LookupCount    int64     // 查询操作计数
	FalsePositive  int64     // 误报计数
	LastExpandTime time.Time // 最后一次扩容时间
}

// BlacklistUser 数据库模型
type BlacklistUser struct {
	UserID string `gorm:"primaryKey;size:64"` // 用户ID，主键，长度64
}

// NewUserBlacklist 创建黑名单过滤器
// ctx: 上下文，用于控制超时和取消
// config: 配置参数
// 返回值: 黑名单实例和错误信息
func NewUserBlacklist(ctx context.Context, config Config) (*UserBlacklist, error) {
	// 初始化GORM连接，设置日志级别为Info
	db, err := gorm.Open(mysql.Open(config.MySQLDSN), &gorm.Config{
		Logger: logger.默认值.LogMode(logger.Info),
	})
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %w", err)
	}

	// 设置连接池参数
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("获取数据库连接失败: %w", err)
	}
	sqlDB.SetMaxOpenConns(10)           // 最大打开连接数
	sqlDB.SetMaxIdleConns(5)            // 最大空闲连接数
	sqlDB.SetConnMaxLifetime(time.Hour) // 连接最大存活时间

	// 自动迁移表结构
	if err := db.WithContext(ctx).AutoMigrate(&BlacklistUser{}); err != nil {
		return nil, fmt.Errorf("自动迁移表结构失败: %w", err)
	}

	// 初始化布谷鸟过滤器
	cf := Cuckoo.NewCuckooFilter(config.FilterCapacity, config.FalsePositiveRate)
	if cf == nil {
		return nil, fmt.Errorf("创建过滤器失败")
	}

	return &UserBlacklist{
		filter: cf,
		db:     db,
		config: config,
	}, nil
}

// LoadFromDB 从数据库加载黑名单
// ctx: 上下文，用于控制超时和取消
// 返回值: 错误信息
func (ubl *UserBlacklist) LoadFromDB(ctx context.Context) error {
	ubl.mu.Lock()         // 获取写锁
	defer ubl.mu.Unlock() // 确保锁释放

	// 查询所有黑名单用户
	var users []BlacklistUser
	if err := ubl.db.WithContext(ctx).Find(&users).Error; err != nil {
		return fmt.Errorf("查询黑名单失败: %w", err)
	}

	// 将用户ID添加到过滤器中
	for _, user := range users {
		if !ubl.filter.Insert([]byte(user.UserID)) {
			log.Printf("警告: 添加用户 %s 到过滤器失败", user.UserID)
		}
	}
	log.Printf("成功加载 %d 个黑名单用户", len(users))
	return nil
}

// IsBlacklisted 检查用户是否在黑名单中
func (ubl *UserBlacklist) IsBlacklisted(userID string) bool {
	ubl.mu.RLock()
	defer ubl.mu.RUnlock()
	return ubl.filter.Lookup([]byte(userID))
}

// AddToBlacklist 添加用户到黑名单
func (ubl *UserBlacklist) AddToBlacklist(userID string) error {
	ubl.mu.Lock()
	defer ubl.mu.Unlock()

	// 使用事务保证数据一致性
	err := ubl.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&BlacklistUser{UserID: userID}).Error; err != nil {
			return err
		}

		if !ubl.filter.Insert([]byte(userID)) {
			return fmt.Errorf("过滤器插入失败，可能已达容量上限")
		}
		return nil
	})

	if err != nil {
		log.Printf("添加黑名单失败: %v", err)
	}
	return err
}

// RemoveFromBlacklist 从黑名单移除用户
func (ubl *UserBlacklist) RemoveFromBlacklist(userID string) error {
	ubl.mu.Lock()
	defer ubl.mu.Unlock()

	// 使用事务保证数据一致性
	err := ubl.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Delete(&BlacklistUser{}, "user_id = ?", userID).Error; err != nil {
			return err
		}

		ubl.filter.Delete([]byte(userID))
		return nil
	})

	if err != nil {
		log.Printf("移除黑名单失败: %v", err)
	}
	return err
}

// BatchAddToBlacklist 批量添加用户到黑名单
func (ubl *UserBlacklist) BatchAddToBlacklist(userIDs []string) error {
	ubl.mu.Lock()
	defer ubl.mu.Unlock()

	return ubl.db.Transaction(func(tx *gorm.DB) error {
		for _, userID := range userIDs {
			if err := tx.Create(&BlacklistUser{UserID: userID}).Error; err != nil {
				return err
			}
			if !ubl.filter.Insert([]byte(userID)) {
				return fmt.Errorf("添加用户 %s 到过滤器失败", userID)
			}
			ubl.stats.InsertCount++
		}
		return nil
	})
}

// MonitorFilter 监控过滤器状态
func (ubl *UserBlacklist) MonitorFilter(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ubl.checkAndExpand()
		}
	}
}

// checkAndExpand 检查并扩展过滤器容量
// 当过滤器使用率达到80%时自动扩容为当前容量的2倍
func (ubl *UserBlacklist) checkAndExpand() {
	ubl.mu.Lock()
	defer ubl.mu.Unlock()

	// 计算当前使用率
	usageRate := float64(ubl.filter.Count()) / float64(ubl.config.FilterCapacity)
	if usageRate > 0.8 {
		// 创建新过滤器，容量翻倍
		newCapacity := ubl.config.FilterCapacity * 2
		newFilter := Cuckoo.NewCuckooFilter(newCapacity, ubl.config.FalsePositiveRate)

		// 从数据库重新加载所有数据到新过滤器
		var users []BlacklistUser
		if err := ubl.db.Find(&users).Error; err == nil {
			for _, user := range users {
				newFilter.Insert([]byte(user.UserID))
			}
		}

		// 替换旧过滤器
		ubl.filter = newFilter
		ubl.config.FilterCapacity = newCapacity
		ubl.stats.LastExpandTime = time.Now()
		log.Printf("过滤器已扩容至 %d", newCapacity)
	}
}

// GetStats 获取性能统计
func (ubl *UserBlacklist) GetStats() Stats {
	ubl.mu.RLock()
	defer ubl.mu.RUnlock()
	return ubl.stats
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := Config{
		MySQLDSN:          "root:123456@tcp(127.0.0.1:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local",
		FilterCapacity:    100000,
		FalsePositiveRate: 0.03,
	}

	blacklist, err := NewUserBlacklist(ctx, config)
	if err != nil {
		log.Fatal(err)
	}

	// 启动监控协程
	go blacklist.MonitorFilter(ctx)

	// 使用示例
	userID := "user123"
	if blacklist.IsBlacklisted(userID) {
		fmt.Printf("用户 %s 在黑名单中\n", userID)
	} else {
		fmt.Printf("用户 %s 不在黑名单中\n", userID)
	}
}
