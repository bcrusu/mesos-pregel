package parser

import (
	"bufio"
	"io"
	"strconv"
	"strings"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/encoding"
	"github.com/bcrusu/mesos-pregel/protos"
)

type dimacsVertexParser struct {
	lineScanner *bufio.Scanner
}

type dimacsEdgeParser struct {
	lineScanner *bufio.Scanner
}

func NewDimacsVertexParser(reader io.Reader) VertexParser {
	return &dimacsVertexParser{getLineScanner(reader)}
}

func NewDimacsEdgeParser(reader io.Reader) EdgeParser {
	return &dimacsEdgeParser{getLineScanner(reader)}
}

func (parser *dimacsVertexParser) Next() (*pregel.Vertex, error) {
	for {
		success := parser.lineScanner.Scan()
		if !success {
			return nil, parser.lineScanner.Err()
		}

		vertex, success := parseVertex(parser.lineScanner.Text())
		if success {
			return vertex, nil
		}
	}
}

func (parser *dimacsEdgeParser) Next() (*pregel.Edge, error) {
	for {
		success := parser.lineScanner.Scan()
		if !success {
			return nil, parser.lineScanner.Err()
		}

		edge, success := parseEdge(parser.lineScanner.Text())
		if success {
			return edge, nil
		}
	}
}

func getLineScanner(reader io.Reader) *bufio.Scanner {
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

	value, err := strconv.Atoi(splits[3])
	if err != nil {
		return nil, false
	}

	valueBytes, err := encoding.Int32ValueEncoder().Marshal(&protos.Int32Value{Value: int32(value)})
	if err != nil {
		panic("failed to marshal edge value")
	}

	return &pregel.Edge{
		From:  splits[1],
		To:    splits[2],
		Value: valueBytes,
	}, true
}

func parseVertex(text string) (edge *pregel.Vertex, success bool) {
	if text[0] != 'v' {
		return nil, false
	}

	splits := strings.Split(text, " ")
	if len(splits) != 4 {
		return nil, false
	}

	x, err := strconv.Atoi(splits[2])
	if err != nil {
		return nil, false
	}

	y, err := strconv.Atoi(splits[3])
	if err != nil {
		return nil, false
	}

	valueBytes, err := encoding.Coordinate2DValueEncoder().Marshal(&protos.Coordinate2DValue{X: int32(x), Y: int32(y)})
	if err != nil {
		panic("failed to marshal edge value")
	}

	return &pregel.Vertex{
		ID:    splits[1],
		Value: valueBytes,
	}, true
}
