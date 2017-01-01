package partitioner

// TODO:
// https://github.com/datastax/java-driver/blob/3.x/driver-core/src/main/java/com/datastax/driver/core/Metadata.java#L747
// https://github.com/datastax/spark-cassandra-connector/blob/master/spark-cassandra-connector/src/main/scala/com/datastax/spark/connector/rdd/partitioner/CassandraPartitionGenerator.scala

type CassandraPartitioner struct {
}

func NewCassandraPartitioner() *CassandraPartitioner {
	result := new(CassandraPartitioner)
	return result
}
