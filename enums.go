package goworker

type Status string

const (
	Processed Status = "stat:processed"
	Failed    Status = "stat:failed"
)
