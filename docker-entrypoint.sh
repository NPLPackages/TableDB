#!/bin/bash
set -e

if [ "$TABLEDB_MODE" = 'server' ]; then
	
	if [ "$SERVER_ID" ]; then
		echo $SERVER_ID >> ./config.properties
		npl -d bootstrapper="npl_mod/TableDBApp/App.lua" servermode="true" dev="./" raftMode="server" threadName="rtdb" baseDir=""
	fi

fi

if [ "$TABLEDB_MODE" = 'client' ]; then 

	npl -d bootstrapper="npl_mod/TableDBApp/App.lua" servermode="true" dev="./" raftMode="client" baseDir="" clientMode="appendEntries" 
fi

