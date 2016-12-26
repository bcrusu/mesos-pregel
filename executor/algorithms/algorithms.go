package algorithms

import (
	"github.com/bcrusu/pregel"
)

type Algorithm interface {
	Combine(first pregel.Message, second pregel.Message) pregel.Message
}
