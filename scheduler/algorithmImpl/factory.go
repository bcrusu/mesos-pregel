package algorithmImpl

import "fmt"

func NewAlgorithm(name string, initState []byte) (Algorithm, error) {
	var algorithm Algorithm
	var err error

	switch name {
	case "ShortestPath":
		algorithm = nil
	default:
		err = fmt.Errorf("Invalid algorithm name '%s'", name)
	}

	return algorithm, err
}
