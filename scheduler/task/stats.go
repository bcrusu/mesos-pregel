package task

import (
	"time"

	"github.com/bcrusu/mesos-pregel/protos"
)

type Stats struct {
	TotalDuration     time.Duration
	ComputedCount     int32
	ComputeDuration   time.Duration
	SentMessagesCount int32
	HaltedCount       int32
	InactiveCount     int32
}

func (s *Stats) add(stats *protos.ExecSuperstepResult_Stats) {
	s.TotalDuration += stats.TotalDuration
	s.ComputedCount += stats.ComputedCount
	s.ComputeDuration += stats.ComputeDuration
	s.SentMessagesCount += stats.SentMessagesCount
	s.HaltedCount += stats.HaltedCount
	s.InactiveCount += stats.InactiveCount
}
