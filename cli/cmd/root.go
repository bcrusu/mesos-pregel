package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/bcrusu/mesos-pregel/cli/api"
	"github.com/bcrusu/mesos-pregel/protos"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/glog"
	"github.com/spf13/cobra"
)

var (
	apiAddress                  = PregelRootCmd.PersistentFlags().String("server", "localhost:6060", "Pregel API service addresss")
	storeType                   = PregelRootCmd.PersistentFlags().String("store", "Cassandra", "Store type. Available stores: Cassandra")
	cassandraHosts              = PregelRootCmd.PersistentFlags().String("cassandra.hosts", "", "Cassandra hosts separated by ','")
	cassandraKeyspace           = PregelRootCmd.PersistentFlags().String("cassandra.keyspace", "pregel", "Cassandra keyspace")
	cassandraReplicationFactor  = PregelRootCmd.PersistentFlags().Int("cassandra.replication_factor", 3, "Cassandra keyspace replication factor")
	cassandraVerticesTable      = PregelRootCmd.PersistentFlags().String("cassandra.vertices_table", "", "Cassandra vertices table name")
	cassandraEdgesTable         = PregelRootCmd.PersistentFlags().String("cassandra.edges_table", "", "Cassandra edges table name")
	cassandraWriteBatchMaxSize  = PregelRootCmd.PersistentFlags().Int("cassandra.writeBatch_maxSize", 2000, "Cassandra write batch max size")
	cassandraWriteBatchMaxBytes = PregelRootCmd.PersistentFlags().Int("cassandra.writeBatch_maxBytes", 30*1024, "Cassandra write batch max size in bytes")

	jobID = jobCmd.PersistentFlags().StringP("jobID", "j", "", "The Pregel Job ID")
)

func init() {
	jobCmd.AddCommand(cancelCmd)
	jobCmd.AddCommand(statsCmd)
	jobCmd.AddCommand(resultCmd)

	PregelRootCmd.AddCommand(startCmd)
	PregelRootCmd.AddCommand(jobCmd)
	PregelRootCmd.AddCommand(listCmd)
}

var PregelRootCmd = &cobra.Command{
	Use:          "pregel",
	SilenceUsage: true,
}

var jobCmd = &cobra.Command{
	Use:   "job",
	Short: "Interacts with an existing job.",
}

func processStoreFlags() (store string, storeParams []byte, err error) {
	var message proto.Message

	switch *storeType {
	case "Cassandra":
		hosts := strings.Split(*cassandraHosts, ",")
		message = &protos.CassandraStoreParams{
			Hosts:             hosts,
			Keyspace:          *cassandraKeyspace,
			ReplicationFactor: int32(*cassandraReplicationFactor),
			VerticesTable:     *cassandraVerticesTable,
			EdgesTable:        *cassandraEdgesTable,
			Timeout:           int64(time.Minute),
			BatchOptions: &protos.CassandraStoreParams_BatchOptions{
				MaxSize:  int32(*cassandraWriteBatchMaxSize),
				MaxBytes: int32(*cassandraWriteBatchMaxBytes),
			},
		}
	default:
		return "", nil, fmt.Errorf("unknown store type %s", *storeType)
	}

	bytes, err := proto.Marshal(message)
	if err != nil {
		return "", nil, fmt.Errorf("failed to marshal store params")
	}

	return *storeType, bytes, nil
}

func useAPI(action func(client *api.Client) error) error {
	client := api.New()
	if err := client.Dial(*apiAddress); err != nil {
		return err
	}

	defer func() {
		if err := client.Close(); err != nil {
			glog.Warningf("failed to close API connection; error=%v", err)
		}
	}()

	return action(client)
}

func getCallStatusName(status protos.CallStatus) string {
	name, ok := protos.CallStatus_name[int32(status)]
	if !ok {
		return "unknown"
	}

	return name
}
