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
	Set(key string, value interface{}) error
	Lpush(key string, value interface{}) error
	Lpop(key string) (reply interface{}, err error)
	Rpush(key string, value interface{}) error
	Rpop(key string, value interface{}) error
	Incr(key string) error
	Sadd(key string, value interface{}) error
	Srem(key string, value interface{}) error
	Del(key string) error
	Flush() error
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
