#!/bin/bash
protoc -I messages/ messages/message.proto --go_out=plugins=grpc:messages
