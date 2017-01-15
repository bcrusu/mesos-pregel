package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/bcrusu/mesos-pregel"
	_ "github.com/bcrusu/mesos-pregel/cassandra" // register Cassandra store
	"github.com/bcrusu/mesos-pregel/dataLoader/parser"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/bcrusu/mesos-pregel/store"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

func main() {
	flag.Parse()

	log.Println("Running...")

	if err := run(); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

	log.Println("Done.")
}

func run() error {
	store, err := getStore()
	if err != nil {
		return err
	}
	defer store.Close()

	if VerticesFilePath != nil {
		file, err := os.Open(*VerticesFilePath)
		if err != nil {
			return err
		}
		defer file.Close()

		parser, err := parser.NewVertexParser(*Parser, file)
		if err != nil {
			return err
		}

		loadVertices(parser, store)
	}

	if EdgesFilePath != nil {
		file, err := os.Open(*EdgesFilePath)
		if err != nil {
			return err
		}
		defer file.Close()

		parser, err := parser.NewEdgeParser(*Parser, file)
		if err != nil {
			return err
		}

		loadEdges(parser, store)
	}

	return nil
}

func loadVertices(parser parser.VertexParser, store store.GraphStore) error {
	batch := make([]*pregel.Vertex, 0, *ReadBatchSize)
	batchNo := 0

	for true {
		vertex, err := parser.Next()
		if err != nil {
			return errors.Wrapf(err, "vertex parser error")
		}

		if vertex != nil {
			batch = append(batch, vertex)
		}

		// if batch is full OR done parsing
		if len(batch) == cap(batch) || vertex == nil {
			batchNo++
			log.Printf("Writing vertex batch %d containing %d items", batchNo, len(batch))

			if err := store.SaveVertices(batch); err != nil {
				return err
			}

			batch = batch[:0]
		}

		if vertex == nil {
			break
		}
	}

	return nil
}

func loadEdges(parser parser.EdgeParser, store store.GraphStore) error {
	batch := make([]*pregel.Edge, 0, *ReadBatchSize)
	batchNo := 0

	for true {
		edge, err := parser.Next()
		if err != nil {
			return errors.Wrapf(err, "edge parser error")
		}

		if edge != nil {
			batch = append(batch, edge)
		}

		// if batch is full OR done parsing
		if len(batch) == cap(batch) || edge == nil {
			batchNo++
			log.Printf("Writing edge batch %d containing %d items", batchNo, len(batch))

			if err := store.SaveEdges(batch); err != nil {
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

func getStore() (store.GraphStore, error) {
	store, err := store.New(*Store, getStoreParams())
	if err != nil {
		return nil, err
	}

	if err = store.Connect(); err != nil {
		return nil, err
	}

	if err = store.Init(); err != nil {
		return nil, err
	}

	return store, nil
}

func getStoreParams() []byte {
	var message proto.Message

	switch *Store {
	case "Cassandra":
		hosts := strings.Split(*CassandraHosts, ",")
		message = &protos.CassandraStoreParams{
			Hosts:             hosts,
			Keyspace:          *CassandraKeyspace,
			ReplicationFactor: int32(*CassandraReplicationFactor),
			VerticesTable:     *CassandraVerticesTable,
			EdgesTable:        *CassandraEdgesTable,
			TimeoutSec:        60,
			BatchOptions: &protos.CassandraStoreParams_BatchOptions{
				MaxSize:  2000,
				MaxBytes: 30 * 1024,
			},
		}
	default:
		panic("unknown store type")
	}

	bytes, err := proto.Marshal(message)
	if err != nil {
		panic("failed to marshal store params")
	}

	return bytes
}
