package parsers

import (
	"bufio"
	"io"
	"strconv"
	"strings"

	"github.com/bcrusu/pregel"
)

type DimacsParser struct {
	lineScanner *bufio.Scanner
}

func NewDimacsParser(reader io.Reader) *DimacsParser {
	result := new(DimacsParser)
	result.lineScanner = getScanner(reader)
	return result
}

func (parser *DimacsParser) Next() *pregel.Edge {
	for true {
		success := parser.lineScanner.Scan()
		if !success {
			return nil
		}

		edge, success := parseEdge(parser.lineScanner.Text())
		if success {
			return edge
		}
	}

	panic("unreachable")
}

func getScanner(reader io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)

	return scanner
}

func parseEdge(text string) (edge *pregel.Edge, success bool) {
	if text[0] != 'a' {
		return nil, false
	}

	splits := strings.Split(text, " ")
	if len(splits) != 4 {
		return nil, false
	}

	weight, err := strconv.Atoi(splits[3])
	if err != nil {
		return nil, false
	}

	result := new(pregel.Edge)
	result.From = splits[1]
	result.To = splits[2]
	result.Weight = weight

	return result, true
}
