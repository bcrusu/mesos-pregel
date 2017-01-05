package parser

import (
	"bufio"
	"io"
	"strconv"
	"strings"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/encoding"
)

type DimacsParser struct {
	lineScanner  *bufio.Scanner
	valueEncoder encoding.Encoder
}

func NewDimacsParser(reader io.Reader) *DimacsParser {
	result := new(DimacsParser)
	result.lineScanner = getScanner(reader)
	result.valueEncoder = encoding.Int32ValueEncoder()
	return result
}

func (parser *DimacsParser) Next() *pregel.Edge {
	for {
		success := parser.lineScanner.Scan()
		if !success {
			return nil
		}

		edge, success := parser.parseEdge(parser.lineScanner.Text())
		if success {
			return edge
		}
	}
}

func getScanner(reader io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)

	return scanner
}

func (parser *DimacsParser) parseEdge(text string) (edge *pregel.Edge, success bool) {
	if text[0] != 'a' {
		return nil, false
	}

	splits := strings.Split(text, " ")
	if len(splits) != 4 {
		return nil, false
	}

	value, err := strconv.Atoi(splits[3])
	if err != nil {
		return nil, false
	}

	result := new(pregel.Edge)
	result.From = splits[1]
	result.To = splits[2]
	result.Value = parser.marshalValue(value)

	return result, true
}

func (parser *DimacsParser) marshalValue(value int) []byte {
	bytes, _ := parser.valueEncoder.Marshal(value)
	return bytes
}
