#!/bin/bash

mockgen github.com/percona/mongodb-backup/grpc/messages  Messages_MessagesChatClient > mock_messages/messages_chat_client.go
mockgen github.com/percona/mongodb-backup/grpc/messages  Messages_MessagesChatClient > mock_messages/messages_chat_client.go
mockgen github.com/percona/mongodb-backup/grpc/messages  MessagesClient > mock_messages/messages_client.go
mockgen github.com/percona/mongodb-backup/grpc/messages  MessagesServer > mock_messages/messages_server.go
mockgen github.com/percona/mongodb-backup/grpc/messages  Messages_MessagesChatServer > mock_messages/messages_chat_server.go
