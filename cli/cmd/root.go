package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
	"github.com/spf13/cobra"
)

var (
	apiAddress             = PregelRootCmd.PersistentFlags().String("server", "localhost:6060", "Pregel API service addresss")
	storeType              = PregelRootCmd.PersistentFlags().String("store", "Cassandra", "Store type. Available stores: Cassandra")
	cassandraHosts         = PregelRootCmd.PersistentFlags().String("cassandra.hosts", "", "Cassandra hosts separated by ','")
	cassandraKeyspace      = PregelRootCmd.PersistentFlags().String("cassandra.keyspace", "pregel", "Cassandra keyspace")
	cassandraVerticesTable = PregelRootCmd.PersistentFlags().String("cassandra.vertices_table", "", "Cassandra vertices table name")
	cassandraEdgesTable    = PregelRootCmd.PersistentFlags().String("cassandra.edges_table", "", "Cassandra edges table name")

	jobID = jobCmd.PersistentFlags().StringP("jobID", "j", "", "The Pregel Job ID")
)

func init() {
	jobCmd.AddCommand(cancelCmd)
	jobCmd.AddCommand(statusCmd)
	jobCmd.AddCommand(resultCmd)

	PregelRootCmd.AddCommand(startCmd)
	PregelRootCmd.AddCommand(jobCmd)
}

var PregelRootCmd = &cobra.Command{
	Use: "pregel",
}

var jobCmd = &cobra.Command{
	Use:   "job",
	Short: "Interacts with an existing job.",
}

func getStoreParams() ([]byte, error) {
	var message proto.Message

	switch *storeType {
	case "Cassandra":
		hosts := strings.Split(*cassandraHosts, ",")
		message = &protos.CassandraStoreParams{
			Hosts:             hosts,
			Keyspace:          *cassandraKeyspace,
			ReplicationFactor: 3,
			VerticesTable:     *cassandraVerticesTable,
			EdgesTable:        *cassandraEdgesTable,
			Timeout:           int64(time.Minute),
			BatchOptions: &protos.CassandraStoreParams_BatchOptions{
				MaxSize:  2000,
				MaxBytes: 30 * 1024,
			},
		}
	default:
		return nil, fmt.Errorf("unknown store type %s", *storeType)
	}

	bytes, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal store params")
	}

	return bytes, nil
}
