@echo off
cd /d d:/code/npl/TableDB/setup
set curdir=%~dp0
if "%1" == "dummy" goto setupDummy
if "%1" == "client" goto setupClient

for %%i in (1 2 3) do (
    rm -r "%curdir%\server%%i"
    mkdir "%curdir%\server%%i"
    copy /Y init-cluster.json "%curdir%\server%%i\cluster.json"
    copy /Y "..\sqlite.dll" "%curdir%\server%%i\sqlite.dll"
    echo server.id=%%i> "%curdir%\server%%i\config.properties"
    echo start server%%i
    REM start "server%%i" /D "%curdir%\server%%i" npl -d bootstrapper="npl_mod/TableDBApp/App.lua" servermode="true" dev="../../" raftMode="server" threadName="rtdb" baseDir="./"
    start "server%%i" /D "%curdir%\server%%i" npl bootstrapper="npl_mod/TableDBApp/App.lua" servermode="true" dev="../../" raftMode="server" threadName="rtdb" baseDir="./"
)

goto done
:setupClient
echo start a client
mkdir client
copy /Y init-cluster.json "%curdir%\client\cluster.json"
copy /Y "..\sqlite.dll" "%curdir%\client\sqlite.dll"
copy /Y "%curdir%\server1\config.properties" "%curdir%\client\config.properties"
REM start "client" /D "%curdir%\client" npl -d bootstrapper="npl_mod/TableDBApp/App.lua" servermode="true" dev="../../" raftMode="client" baseDir="./" clientMode="%2" serverId="%3"
start "client" /D "%curdir%\client" npl bootstrapper="npl_mod/TableDBApp/App.lua" servermode="true" dev="../../" raftMode="client" baseDir="./" clientMode="%2" serverId="%3"


goto done
:setupDummy
mkdir "%curdir%\dummys"
echo start dummy server
start "Dummy Server" /D "%curdir%\dummys" npl -d bootstrapper="npl_mod/TableDBApp/App.lua" servermode="true" dev="../../" raftMode="dummy" dummyMode="server" baseDir=""
mkdir "%curdir%\dummyc"
echo start dummy client
start "Dummy Client" /D "%curdir%\dummyc" npl -d bootstrapper="npl_mod/TableDBApp/App.lua" servermode="true" dev="../../" raftMode="dummy" dummyMode="client" baseDir=""
:done
@echo on