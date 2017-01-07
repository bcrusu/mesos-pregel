#!/bin/bash

CASSANDRA_VERSION="3.9"
CASSANDRA_CLUSTER_NAME="pregel"
CASSANDRA_NODE_COUNT=2

create_cluster() {
	ccm create $CASSANDRA_CLUSTER_NAME -v $CASSANDRA_VERSION
	ccm populate -I 127.0.100.%d --vnodes -n $CASSANDRA_NODE_COUNT
}

start_cluster() {
	ccm switch $CASSANDRA_CLUSTER_NAME
    ccm start --jvm_arg="-Xms256M -Xmx256M"
}

stop_cluster() {
	ccm stop $CASSANDRA_CLUSTER_NAME
}

clean() {
	ccm remove $CASSANDRA_CLUSTER_NAME
}

case "$1" in
	create)
		echo "Creating cluster..."
		create_cluster
		echo "Done."
		;;
	start)
		echo "Starting..."
		start_cluster
		echo "Done."
		;;
	stop)
		echo "Stopping..."
		stop_cluster
		echo "Done."
		;;
	clean)
		echo "Cleaning cluster dir..."
		clean
		echo "Done."
		;;
	*)
		echo "Usage: cluster {start|stop|create|clean}"
		exit 1
		;;
esac
