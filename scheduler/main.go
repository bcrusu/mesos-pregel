package scheduler

import (
	"flag"
	"log"
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
	//TODO
	return nil
}
