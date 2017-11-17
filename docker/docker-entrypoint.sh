#!/bin/sh
set -e

TABLEDB_MODE=$1
SERVER_ID=$2

if [ "$TABLEDB_MODE" = 'server' ]; then
	
	if [ "$SERVER_ID" ]; then
		# echo "server.id=$SERVER_ID" >> ./config.properties
		npl bootstrapper="npl_mod/TableDBApp/App.lua" servermode="true" dev="../../" raftMode="server" threadName="rtdb" baseDir=""
	fi

fi

if [ "$TABLEDB_MODE" = 'client' ]; then 

	npl bootstrapper="npl_mod/TableDBApp/App.lua" servermode="true" dev="../../" raftMode="client" baseDir="" clientMode="appendEntries" 
fi

