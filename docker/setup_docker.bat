@echo off
cd /d d:/code/npl/TableDB/docker
set curdir=%~dp0
if "%1" == "client" goto setupClient

REM use
REM `docker network create --gateway 172.16.0.1 --subnet 172.16.0.0/21 mynet`
REM to create a subnet for the use of --ip in docker run

for %%i in (11 12 13) do (
    echo start server%%i
    rm -r %curdir%\server%%i\
    mkdir "%curdir%\server%%i"
    copy /Y init-cluster.json "%curdir%\server%%i\cluster.json"
    echo server.id=%%i> "%curdir%\server%%i\config.properties"
    docker run --mount type=bind,source="D:/code/npl/TableDB/docker/server%%i",target=/app/tabledb/setup/server/ --network mynet --ip 172.16.0.%%i -d tabledb server %%i
)

goto done

:setupClient
echo start a client
rm -r %curdir%\client\
mkdir "%curdir%\client\temp\test_raft_database\"
copy /Y init-cluster.json "%curdir%\client\cluster.json"
copy /Y tabledb.config.xml "%curdir%\client\temp\test_raft_database\tabledb.config.xml"
echo server.id=11> "%curdir%\client\config.properties"
docker run --mount type=bind,source="D:/code/npl/TableDB/docker/client",target=/app/tabledb/setup/server/ --network mynet --ip 172.16.0.4 -d tabledb client

goto done

:done
@echo on