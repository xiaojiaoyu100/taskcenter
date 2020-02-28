package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
)

const (
	RD_LOCK_KEY         = "taskcenter:lock:key:%d"
	RD_TASK_COUNTER_KEY = "taskcenter:minute:task:count:hash-%s"
	RD_TASK_TOTAL_KEY   = "total"
)

type Cache struct {
	rdb  *redis.Client
	lock sync.Mutex
}

type RedisConfig struct {
	Addr     string
	Pwd      string
	Db       int
	PoolSize int
}

var DefaultCache *Cache

func New(config RedisConfig) *Cache {
	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: config.Pwd,
		DB:       config.Db,
		PoolSize: config.PoolSize,
	})
	return &Cache{rdb: client}
}

func (c *Cache) GetRedisClient() *redis.Client {
	return c.rdb
}

func (c *Cache) GetLock(now int64) bool {
	key := fmt.Sprintf(RD_LOCK_KEY, now)
	suc, err := c.rdb.SetNX(key, 1, 1*time.Second).Result()
	if err != nil {
		logrus.Error("GetLock error err ", err)
		return false
	}
	return suc
}

func (c *Cache) CloseRedis() error {
	return c.rdb.Close()
}

func InitCache(config RedisConfig) {
	if DefaultCache == nil {
		DefaultCache = New(config)
	}
}

/*
统计每一个name(topic) 下面任务的数量
Return: topicTaskCount, totalTaskCount, error
*/
func (c *Cache) IncreaseTaskCount(name string) (int64, int64, error) {
	now := time.Now()
	current := fmt.Sprintf(
		"%d%d%d%d%d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute())
	key := fmt.Sprintf(RD_TASK_COUNTER_KEY, current)

	c.lock.Lock()
	defer c.lock.Unlock()

	var count, totalCount int64
	var err error

	// 最高一次设置之后两分钟后过期
	c.rdb.Expire(key, 120*time.Second)
	count, err = c.rdb.HIncrBy(key, name, 1).Result()
	totalCount, err = c.rdb.HIncrBy(key, RD_TASK_TOTAL_KEY, 1).Result()

	return count, totalCount, err
}

func (c *Cache) GetTaskCount() (map[string]string, error) {
	now := time.Now()
	current := fmt.Sprintf(
		"%d%d%d%d%d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute())
	key := fmt.Sprintf(RD_TASK_COUNTER_KEY, current)
	return c.rdb.HGetAll(key).Result()
}
