#!/bin/bash

if [ -z "$GOPATH" ]; then
	echo "GOPATH not set. Exiting..."
	exit 1
fi

BASE_PATH="${GOPATH}/src/github.com/bcrusu/pregel"

ProtoDirs=("${BASE_PATH}/executor/algorithms/shortestPath")

for protoDir in ${ProtoDirs[@]}; do
	PROTO_PATH=${protoDir}
  	PROTO_PATH=${PROTO_PATH}:${GOPATH}/src/github.com/gogo/protobuf/protobuf
	PROTO_PATH=${PROTO_PATH}:${GOPATH}/src/github.com/gogo/protobuf/gogoproto

	protos=(${protoDir}/*.proto)
	for proto in "${protos[@]}"; do 
		protoc --proto_path=${PROTO_PATH} --gogo_out=${protoDir} ${proto}
	done	
done
