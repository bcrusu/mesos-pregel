package main

import (
	"flag"
	"log"
	"os"

	"github.com/bcrusu/pregel"
	"github.com/bcrusu/pregel/dataLoader/parsers"
	"github.com/bcrusu/pregel/dataLoader/stores"
)

var (
	inputFilePath string
	batchSize     int
)

func init() {
	flag.StringVar(&inputFilePath, "filePath", "", "Input file path")
	flag.IntVar(&batchSize, "batchSize", 1500, "Insert batch size")
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
		return err
	}
	defer store.Close()

	var parser parsers.Parser
	if parser, err = parsers.NewParser(inputFile); err != nil {
		return err
	}

	return loadData(parser, store)
}

func loadData(parser parsers.Parser, store stores.Store) error {
	batch := make([]*pregel.Edge, 0, batchSize)
	batchNo := 0

	for true {
		edge := parser.Next()
		if edge != nil {
			batch = append(batch, edge)
		}

		// if batch is full OR done parsing
		if len(batch) == cap(batch) || edge == nil {
			batchNo++
			log.Printf("Writing batch %d containing %d items", batchNo, len(batch))

			if err := store.Write(batch); err != nil {
				return err
			}

			batch = batch[:0]
		}

		if edge == nil {
			break
		}
	}

	return nil
}
