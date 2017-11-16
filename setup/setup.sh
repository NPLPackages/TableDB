#!/bin/sh


debug() { echo -e "\033[0;37m$*\033[0m"; }
info() { echo -e "\033[0;36m$*\033[0m"; }
error() { >&2  echo -e "\033[0;31m$*\033[0m"; }
fail() { error ${1}; exit ${2:-1}; }


cd /mnt/d/code/npl/TableDB/setup
CURDIR=`pwd`
info "current directory $CURDIR, start $1"

setupServer() {
    for i in 1 2 3; do
        mkdir -p "$CURDIR/server$i"
        cp -f init-cluster.json "$CURDIR/server$i/cluster.json"
        # cp -f "../libsqlite.so" "$CURDIR/server$i/libsqlite.so"
        echo "server.id=$i" > "$CURDIR/server$i/config.properties"
        info "start server$i"
        cd "$CURDIR/server$i" && npl -d bootstrapper="npl_mod/TableDBApp/App.lua" servermode="true" dev="../../" raftMode="server" threadName="rtdb" baseDir="./"
    done
}

clientMode=$2
# serverId=$2
setupClient() {
    info "start a client"
    mkdir -p ./client
    cp -f init-cluster.json "$CURDIR/client/cluster.json"
    # cp -f "../libsqlite.so" "$CURDIR/client/libsqlite.so"
    cp -f "$CURDIR/server1/config.properties" "$CURDIR/client/config.properties"
    cd "$CURDIR/client" && npl -d bootstrapper="npl_mod/TableDBApp/App.lua" servermode="true" dev="../../" raftMode="client" baseDir="./" clientMode="$clientMode" serverId="$serverId"
}


if [ "$1" = "client" ]; then
    setupClient
else
    setupServer
fi
