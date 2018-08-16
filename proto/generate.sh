#!/bin/bash
echo "Generating messages.proto"
protoc -I messages/ messages/message.proto --go_out=plugins=grpc:messages

echo "Generating api.proto"
protoc -I api/ api/api.proto --go_out=plugins=grpc:api
