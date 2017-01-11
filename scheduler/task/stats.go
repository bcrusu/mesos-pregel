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

func (s *Stats) add(stats *protos.Stats) {
	s.TotalDuration += time.Duration(stats.TotalDuration)
	s.ComputedCount += stats.ComputedCount
	s.ComputeDuration += time.Duration(stats.ComputeDuration)
	s.SentMessagesCount += stats.SentMessagesCount
	s.HaltedCount += stats.HaltedCount
	s.InactiveCount += stats.InactiveCount
}

func (s *Stats) toProto() *protos.Stats {
	return &protos.Stats{
		TotalDuration:     s.TotalDuration.Nanoseconds(),
		ComputedCount:     s.ComputedCount,
		ComputeDuration:   s.ComputeDuration.Nanoseconds(),
		SentMessagesCount: s.SentMessagesCount,
		HaltedCount:       s.HaltedCount,
		InactiveCount:     s.InactiveCount,
	}
}

func (s *Stats) fromProto(stats *protos.Stats) *Stats {
	s.TotalDuration = time.Duration(stats.TotalDuration)
	s.ComputedCount = stats.ComputedCount
	s.ComputeDuration = time.Duration(stats.ComputeDuration)
	s.SentMessagesCount = stats.SentMessagesCount
	s.HaltedCount = stats.HaltedCount
	s.InactiveCount = stats.InactiveCount
	return s
}
