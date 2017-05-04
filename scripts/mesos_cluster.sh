#!/bin/bash

SCRIPTDIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd)

MESOS_WORK_DIR_ROOT=/tmp/mesos
MESOS_CLUSTER_DIR=$MESOS_WORK_DIR_ROOT/clusters/pregel
MESOS_FRAMEWORKS_HOME=$MESOS_WORK_DIR_ROOT/frameworks

start_cluster() {
	if [ ! -d $MESOS_CLUSTER_DIR ]; then
		echo "Creating cluster work dir: $MESOS_CLUSTER_DIR ..."
		mkdir -p $MESOS_CLUSTER_DIR
	fi

	copy_files

	# add libmesos.so to LD load path
	export LD_LIBRARY_PATH=/usr/local/lib:${LD_LIBRARY_PATH}

	mesos-master --cluster=pregel --ip=127.0.100.254 --port=5050 --hostname=127.0.100.254 --allocation_interval=1secs --registry=in_memory --quorum=1 --quiet \
		--log_dir=${MESOS_CLUSTER_DIR}/master/logs --work_dir=${MESOS_CLUSTER_DIR}/master/data &

	sleep 1s

	mesos-slave --master=127.0.100.254:5050 --ip=127.0.100.1 --port=5051 --hostname=127.0.100.1 --resources="cpus:2;mem:512" --attributes=name:slave1 --quiet \
		--frameworks_home=${MESOS_FRAMEWORKS_HOME} --log_dir=${MESOS_CLUSTER_DIR}/slave1/logs --work_dir=${MESOS_CLUSTER_DIR}/slave1/data &

	sleep 0.2s

	mesos-slave --master=127.0.100.254:5050 --ip=127.0.100.2 --port=5051 --hostname=127.0.100.2 --resources="cpus:2;mem:512" --attributes=name:slave2 --quiet \
		--frameworks_home=${MESOS_FRAMEWORKS_HOME} --log_dir=${MESOS_CLUSTER_DIR}/slave2/logs --work_dir=${MESOS_CLUSTER_DIR}/slave2/data &

	sleep 0.2s

	mesos-slave --master=127.0.100.254:5050 --ip=127.0.100.3 --port=5051 --hostname=127.0.100.3 --resources="cpus:2;mem:512" --attributes=name:slave3 --quiet \
		--frameworks_home=${MESOS_FRAMEWORKS_HOME} --log_dir=${MESOS_CLUSTER_DIR}/slave3/logs --work_dir=${MESOS_CLUSTER_DIR}/slave3/data &
}

copy_files() {
	if [ ! -f "$SCRIPTDIR/../executor/executor" ]; then
		echo "Could not find executor binary ..."
		exit 1
	fi

	if [ ! -d "$MESOS_FRAMEWORKS_HOME" ]; then
		echo "Creating frameworks dir: $MESOS_FRAMEWORKS_HOME ..."
		mkdir "$MESOS_FRAMEWORKS_HOME"
	fi

	cp "$SCRIPTDIR/../executor/executor" "$MESOS_FRAMEWORKS_HOME/pregel-exe"
}

stop_cluster() {
	killall -q mesos-slave
	killall -q mesos-master
}

clean() {
	echo "Removing work dir at: $MESOS_WORK_DIR_ROOT ..."
	rm -rf $MESOS_WORK_DIR_ROOT
}


if [ "$(id -u)" != "0" ]; then
   echo "Mesos requires to be executed as root."
   exit 1
fi

case "$1" in
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
		echo "Usage: cluster {start|stop|clean}"
		exit 1
		;;
esac
