package main

import "flag"

var (
	MesosMaster       = flag.String("mesos.master", "127.0.100.1:5050", "Master address <ip:port>")
	ExecutorPath      = flag.String("executorPath", "./executor", "Path to Pregel executor")
	JobStore          = flag.String("job.store", "cassandra", "Job Store")
	CassandraHosts    = flag.String("cassandra.hosts", "127.0.100.1,127.0.100.2", "Cassandra hosts separated by ','")
	CassandraKeyspace = flag.String("cassandra.keyspace", "pregel", "Cassandra keyspace")
)
