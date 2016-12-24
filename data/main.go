package data

import (
	"flag"
	"log"
	"os"

	"github.com/bcrusu/pregel/data/parsers"
)

var (
	inputFilePath *string
)

func init() {
	flag.StringVar(inputFilePath, "filePath", "", "Input file path")
}

func main() {
	log.Println("Running...")

	if err := run(); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

	log.Println("Done.")
}

func run() error {
	flag.Parse()

	inputFile, err := os.Open(*inputFilePath)
	if err != nil {
		return err
	}

	// TODO:
	// var store stores.Store
	// if store, err = stores.NewStore(); err != nil {
	// 	return err
	// }

	var parser parsers.Parser
	if parser, err = parsers.NewParser(inputFile); err != nil {
		return err
	}

	for true {
		edge, success := parser.Next()
		if !success {
			return nil
		}

		log.Println(edge.FromNode)
	}

	return nil
}
