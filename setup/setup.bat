@echo off
set curdir=%~dp0
if "%1" == "dummy" goto setupDummy
if "%1" == "client" goto setupClient

for %%i in (1 2 3) do (
    mkdir "%curdir%\server%%i"
    copy /Y init-cluster.json "%curdir%\server%%i\cluster.json"
    echo server.id=%%i> "%curdir%\server%%i\config.properties"
    echo start server%%i
    start "server%%i" /D "%curdir%\server%%i" npl -d bootstrapper="script/app/App.lua" servermode="true" dev="../../" raftMode="server" baseDir="/" mpPort="800%%i"
)

REM goto done
:setupClient
echo start a client
mkdir client
copy /Y init-cluster.json "%curdir%\client\cluster.json"
copy /Y "%curdir%\server1\config.properties" "%curdir%\client\config.properties"
start "client" /D "%curdir%\client" npl -d bootstrapper="script/app/App.lua" servermode="true" dev="../../" raftMode="client" baseDir="/" "%curdir%\client"


goto done
:setupDummy
mkdir "%curdir%\dummys"
echo start dummy server
start "Dummy Server" /D "%curdir%\dummys" java -jar %curdir%\dmprinter.jar dummy server
mkdir "%curdir%\dummyc"
echo start dummy client
start "Dummy Client" /D "%curdir%\dummyc" java -jar %curdir%\dmprinter.jar dummy client
:done
@echo on
