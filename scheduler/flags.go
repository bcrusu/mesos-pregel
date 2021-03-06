package main

import "flag"

var (
	APIPort                    = flag.Int("port", 6060, "Pregel API port")
	MesosMaster                = flag.String("mesos.master", "127.0.100.1:5050", "Master address <ip:port>")
	JobStore                   = flag.String("job.store", "Cassandra", "Job Store")
	CassandraHosts             = flag.String("cassandra.hosts", "127.0.100.1,127.0.100.2", "Cassandra hosts separated by ','")
	CassandraKeyspace          = flag.String("cassandra.keyspace", "pregel", "Cassandra keyspace")
	CassandraReplicationFactor = flag.Int("cassandra.replication_factor", 3, "Cassandra keyspace replication factor")
)
