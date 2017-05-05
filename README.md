## mesos-pregel: A distributed graph processing framework

### Usage

todo

### Items left to do:

- [ ] More testing
- [ ] Complete checkpointing mechanism (during superstep execution and beforee a new superstep is started)
- [ ] Aggregators type conversion (e.g. SumInt/MaxInt/MinInt aggregators should accept all integer data types, not only int64)
- [ ] Cassandra replication strategies (atm. the keyspace replication strategy is ignored - see "replicationStrategy.go")
- [ ] Better task scheduling logic (e.g. keep track of past performance and failures for each slave node)
- [ ] ...

### Inspiration

[Pregel](http://www.dcs.bbk.ac.uk/~dell/teaching/cc/paper/sigmod10/p135-malewicz.pdf): A System for Large-Scale Graph Processing – Malewicz et al. (Google) 2010 
