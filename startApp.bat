@echo off
rem run in development mode
npl -d bootstrapper="script/App.lua" servermode="true" dev="%~dp0" serverId="2"

