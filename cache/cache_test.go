package cache

import (
	"testing"
	"time"
)

func TestGetLock(t *testing.T) {
	InitCache(RedisConfig{
		Addr:     "127.0.0.1:6379",
		Pwd:      "",
		Db:       0,
		PoolSize: 100,
	})

	now := time.Now().Unix()
	if DefaultCache.GetLock(now) == true {
		t.Log("GetLock is right")
	} else {
		t.Fatal("GetLock is wrong!")
	}

	if DefaultCache.GetLock(now) == false {
		t.Log("GetLock is right")
	} else {
		t.Fatal("GetLock is wrong!")
	}
}

func TestCache_IncreaseTaskCount(t *testing.T) {
	InitCache(RedisConfig{
		Addr:     "127.0.0.1:6379",
		Pwd:      "",
		Db:       0,
		PoolSize: 100,
	})

	name, topic, _ := DefaultCache.IncreaseTaskCount("test1")
	if name == 1 {
		t.Log("add topic test1 is right")
	} else {
		t.Fail()
	}

	if topic == 1 {
		t.Log("add topic test2 is wrong")
	} else {
		t.Fail()
	}

	name, topic, _ = DefaultCache.IncreaseTaskCount("test2")
	if name == 1 {
		t.Log("add topic test1 is right")
	} else {
		t.Fail()
	}

	if topic == 2 {
		t.Log("add topic test2 is wrong")
	} else {
		t.Fail()
	}
}
