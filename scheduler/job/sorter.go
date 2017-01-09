package job

import (
	"sort"

	"github.com/bcrusu/mesos-pregel"
)

func sortJobsByCreationTime(jobs []*pregel.Job) {
	sorter := &jobSorterByCreationTime{jobs}
	sort.Sort(sorter)
}

type jobSorterByCreationTime struct {
	jobs []*pregel.Job
}

func (s *jobSorterByCreationTime) Len() int {
	return len(s.jobs)
}

func (s *jobSorterByCreationTime) Less(i, j int) bool {
	return s.jobs[i].CreationTime.Before(s.jobs[j].CreationTime)
}

func (s *jobSorterByCreationTime) Swap(i, j int) {
	s.jobs[i], s.jobs[j] = s.jobs[j], s.jobs[i]
}
