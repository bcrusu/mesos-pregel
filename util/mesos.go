package util

import (
	"fmt"
	"strings"
)

func GetMesosTaskID(jobID string, pregelTaskID string) string {
	return fmt.Sprintf("job=%s:task=%s", jobID, pregelTaskID)
}

func ParseMesosTaskID(taskID string) (jobID string, pregelTaskID string, success bool) {
	splits := strings.Split(taskID, ":")
	if len(splits) != 2 {
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
