package goworker

import (
	"code.google.com/p/vitess/go/pools"
	"time"
)

var storageEngine storageEngineType

type storageEngineType string

const (
	Resdis storageEngineType = "redis"
)

func init() {
	storageEngine = "redis"
}

type StorageConn interface {
	Set(key string, value interface{})
	Lpush(key string, value interface{})
	Lpop(key string, value interface{}) (err error)
	Rpush(key string, value interface{})
	Rpop(key string, value interface{})
	Incr(key string)
	Sadd(key string, value interface{})
	Srem(key string, value interface{})
	Del(key string)
	Close()
}

func newConnPool(storageEngine storageEngineType) *pools.ResourcePool {
	switch storageEngine {
	case Resdis:
		return newRedisPool(uri, connections, connections, time.Minute)
	default:
		panic("No valid storageEngine provided!")
	}
}
