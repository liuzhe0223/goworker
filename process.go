package goworker

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

type process struct {
	Hostname string
	Pid      int
	ID       string
	Queues   []string
}

func newProcess(id string, queues []string) (*process, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	return &process{
		Hostname: hostname,
		Pid:      os.Getpid(),
		ID:       id,
		Queues:   queues,
	}, nil
}

func (p *process) String() string {
	return fmt.Sprintf("%s:%d-%s:%s", p.Hostname, p.Pid, p.ID, strings.Join(p.Queues, ","))
}

func (p *process) open(conn StorageConn) error {
	conn.Sadd(fmt.Sprintf("%sworkers", namespace), p)
	conn.Set(fmt.Sprintf("%sstat:processed:%v", namespace, p), "0")
	conn.Set(fmt.Sprintf("%sstat:failed:%v", namespace, p), "0")
	conn.Flush()

	return nil
}

func (p *process) close(conn StorageConn) error {
	logger.Infof("%v shutdown", p)
	conn.Srem(fmt.Sprintf("%sworkers", namespace), p)
	conn.Del(fmt.Sprintf("%sstat:processed:%s", namespace, p))
	conn.Del(fmt.Sprintf("%sstat:failed:%s", namespace, p))
	conn.Flush()

	return nil
}

func (p *process) start(conn StorageConn) error {
	conn.Set(fmt.Sprintf("%sworker:%s:started", namespace, p), time.Now().String())
	conn.Flush()

	return nil
}

func (p *process) finish(conn StorageConn) error {
	conn.Del(fmt.Sprintf("%sworker:%s", namespace, p))
	conn.Del(fmt.Sprintf("%sworker:%s:started", namespace, p))
	conn.Flush()

	return nil
}

func (p *process) fail(conn StorageConn) error {
	conn.Incr(fmt.Sprintf("%sstat:failed", namespace))
	conn.Incr(fmt.Sprintf("%sstat:failed:%s", namespace, p))
	conn.Flush()

	return nil
}

func (p *process) queues(strict bool) []string {
	// If the queues order is strict then just return them.
	if strict {
		return p.Queues
	}

	// If not then we want to to shuffle the queues before returning them.
	queues := make([]string, len(p.Queues))
	for i, v := range rand.Perm(len(p.Queues)) {
		queues[i] = p.Queues[v]
	}
	return queues
}
