#!/bin/bash
BASE_DIR=$(git rev-parse --show-toplevel)
echo "Generating messages.proto"
protoc -I. messages/messages.proto --go_out=plugins=grpc:${BASE_DIR}/proto/

echo "Generating api.proto"
protoc -I. api/api.proto --go_out=plugins=grpc:${BASE_DIR}/proto/

