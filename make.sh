#!/bin/sh

GOPATH="`dirname $0 | xargs readlink -f`/Godeps/_workspace:$GOPATH"
go clean
go test
go install
