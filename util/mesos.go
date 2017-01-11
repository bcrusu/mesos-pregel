package util

import (
	"fmt"
	"strings"

	"github.com/pborman/uuid"
)

func GetMesosTaskID(jobID string, pregelTaskID string) string {
	r := uuid.NewRandom().String()
	return fmt.Sprintf("job=%s:task=%s:%s", jobID, pregelTaskID, r)
}

func ParseMesosTaskID(taskID string) (jobID string, pregelTaskID string, success bool) {
	splits := strings.Split(taskID, ":")
	if len(splits) != 3 {
		return "", "", false
	}

	splits2 := strings.Split(splits[0], "=")
	if len(splits) != 2 {
		return "", "", false
	}
	jobID = splits2[1]

	splits2 = strings.Split(splits[1], "=")
	if len(splits) != 2 {
		return "", "", false
	}

	return jobID, splits2[1], true
}
