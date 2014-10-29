package goworker

import (
	"code.google.com/p/vitess/go/pools"
	"errors"
	"github.com/garyburd/redigo/redis"
	"net/url"
	"time"
)

var (
	errorInvalidScheme = errors.New("invalid Redis database URI scheme")
)

type RedisConn struct {
	redis.Conn
}

func (r *RedisConn) Close() {
	_ = r.Conn.Close()
}

func newRedisFactory(uri string) pools.Factory {
	return func() (pools.Resource, error) {
		return redisConnFromUri(uri)
	}
}

func newRedisPool(uri string, capacity int, maxCapacity int, idleTimout time.Duration) *pools.ResourcePool {
	return pools.NewResourcePool(newRedisFactory(uri), capacity, maxCapacity, idleTimout)
}

func redisConnFromUri(uriString string) (*RedisConn, error) {
	uri, err := url.Parse(uriString)
	if err != nil {
		return nil, err
	}

	var network string
	var host string
	var password string
	var db string

	switch uri.Scheme {
	case "redis":
		network = "tcp"
		host = uri.Host
		if uri.User != nil {
			password, _ = uri.User.Password()
		}
		if len(uri.Path) > 1 {
			db = uri.Path[1:]
		}
	case "unix":
		network = "unix"
		host = uri.Path
	default:
		return nil, errorInvalidScheme
	}

	conn, err := redis.Dial(network, host)
	if err != nil {
		return nil, err
	}

	if password != "" {
		_, err := conn.Do("AUTH", password)
		if err != nil {
			conn.Close()
			return nil, err
		}
	}

	if db != "" {
		_, err := conn.Do("SELECT", db)
		if err != nil {
			conn.Close()
			return nil, err
		}
	}

	return &RedisConn{Conn: conn}, nil
}

func (conn *RedisConn) Set(key string, value interface{}) {
	conn.Send("SET", key, value)
}

func (conn *RedisConn) Lpush(key string, value interface{}) {
	conn.Send("LPUSH", key, value)
}

func (conn *RedisConn) Rpush(key string, value interface{}) {
	conn.Send("RPUSH", key, value)
}

func (conn *RedisConn) Lpop(key string) (reply interface{}, err error) {
	reply, err = conn.Do("LPOP", key)
	return
}

func (conn *RedisConn) Incr(key string) {
	conn.Send("INCR", key)
}

func (conn *RedisConn) Sadd(key string, value interface{}) {
	conn.Send("SADD", key, value)
}
