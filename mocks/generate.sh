#!/bin/bash

echo "Generating mocks for Messages_MessagesChatClient"
mockgen github.com/percona/mongodb-backup/grpc/messages  Messages_MessagesChatClient > mock_messages/messages_chat_client.go

echo "Generating mocks for MessagesClient"
mockgen github.com/percona/mongodb-backup/grpc/messages  MessagesClient > mock_messages/messages_client.go

echo "Generating mocks for MessagesServer"
mockgen github.com/percona/mongodb-backup/grpc/messages  MessagesServer > mock_messages/messages_server.go

echo "Generating mocks for Messages_MessagesChatServer"
mockgen github.com/percona/mongodb-backup/grpc/messages  Messages_MessagesChatServer > mock_messages/messages_chat_server.go
