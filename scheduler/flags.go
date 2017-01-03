package main

import "flag"

var JobStore = flag.String("job.store", "cassandra", "Job Store")
var CassandraHosts = flag.String("cassandra.hosts", "127.0.100.1,127.0.100.2", "Cassandra hosts separated by ','")
var CassandraKeyspace = flag.String("cassandra.keyspace", "pregel", "Cassandra keyspace")
