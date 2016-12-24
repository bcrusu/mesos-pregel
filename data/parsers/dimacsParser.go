package parsers

import (
	"bufio"
	"io"
	"strconv"
	"strings"

	"github.com/bcrusu/pregel/data/graph"
)

type DimacsParser struct {
	lineScanner *bufio.Scanner
}

func NewDimacsParser(reader io.Reader) *DimacsParser {
	result := new(DimacsParser)
	result.lineScanner = getScanner(reader)
	return result
}

func (parser *DimacsParser) Next() (edge *graph.Edge, success bool) {
	for true {
		success := parser.lineScanner.Scan()
		if !success {
			return nil, false
		}

		edge, success := parseEdge(parser.lineScanner.Text())
		if success {
			return edge, true
		}
	}

	panic("unreachable")
}

func getScanner(reader io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)

	return scanner
}

func parseEdge(text string) (edge *graph.Edge, success bool) {
	splits := strings.Split(text, " ")
	if len(splits) != 4 || splits[0] != "a" {
		return nil, false
	}

	weight, err := strconv.Atoi(splits[3])
	if err != nil {
		return nil, false
	}

	result := new(graph.Edge)
	result.FromNode = splits[1]
	result.ToNode = splits[2]
	result.Weight = weight

	return result, true
}
