package main

import (
	"flag"
	"log"
	"os"

	"github.com/bcrusu/pregel/data/parsers"
	"github.com/bcrusu/pregel/data/stores"
)

var (
	inputFilePath string
)

func init() {
	flag.StringVar(&inputFilePath, "filePath", "", "Input file path")
}

func main() {
	flag.Parse()

	log.Println("Running...")

	if err := run(); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

	log.Println("Done.")
}

func run() error {
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	var store stores.Store
	if store, err = stores.NewStore(); err != nil {
		return err
	}

	if err = store.Connect(); err != nil {
		return nil
	}
	defer store.Close()

	var parser parsers.Parser
	if parser, err = parsers.NewParser(inputFile); err != nil {
		return err
	}

	for true {
		edge, success := parser.Next()
		if !success {
			return nil
		}

		store.Write(edge)
	}

	return nil
}
