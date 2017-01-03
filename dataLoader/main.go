package main

import (
	"flag"
	"log"
	"os"

	"github.com/bcrusu/mesos-pregel"
	"github.com/bcrusu/mesos-pregel/dataLoader/parser"
	"github.com/bcrusu/mesos-pregel/dataLoader/store"
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

	store, err := store.NewStore()
	if err != nil {
		return err
	}

	if err = store.Connect(); err != nil {
		return err
	}
	defer store.Close()

	parser, err := parser.NewParser(inputFile)
	if err != nil {
		return err
	}

	return loadData(parser, store)
}

func loadData(parser parser.Parser, store store.Store) error {
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
