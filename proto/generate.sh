#!/bin/bash
echo "Generating messages.proto"
protoc -I. messages/messages.proto --go_out=plugins=grpc:$GOPATH/src

echo "Generating api.proto"
protoc -I. api/api.proto --go_out=plugins=grpc:$GOPATH/src



