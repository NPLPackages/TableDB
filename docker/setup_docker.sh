#!/bin/bash

if [ "$1" == "client" ]; then
	echo start a client 
	rm -r ./client
	mkdir -p ./client/temp/test_raft_database
	cp ./init-cluster.json ./client/cluster.json
	cp ./tabledb.config.xml ./client/temp/test_raft_database/tabledb.config.xml
	echo server.id=11 > ./client/config.properties
	docker run -v $PWD/client:/app/tabledb/setup/server/ --network mynet --ip 172.16.0.4 -d xuntian/npl-tabledb client 
else 
	for i in 11 12 13
	do
	echo start server$i
	rm -r ./server$i 
	mkdir ./server$i
	cp ./init-cluster.json ./server$i/cluster.json
	echo server.id=$i > ./server$i/config.properties
	docker run -v $PWD/server$i:/app/tabledb/setup/server/ --network mynet --ip 172.16.0.$i -d xuntian/npl-tabledb server $i
	echo server$i has been ready
	done
fi
