#!/bin/bash

echo "Generating mocks for Messages_MessagesChatClient"
mockgen github.com/percona/percona-backup-mongodb/proto/messages  Messages_MessagesChatClient > mock_messages/messages_chat_client.go

echo "Generating mocks for MessagesClient"
mockgen github.com/percona/percona-backup-mongodb/proto/messages  MessagesClient > mock_messages/messages_client.go

echo "Generating mocks for MessagesServer"
mockgen github.com/percona/percona-backup-mongodb/proto/messages  MessagesServer > mock_messages/messages_server.go

echo "Generating mocks for Messages_MessagesChatServer"
mockgen github.com/percona/percona-backup-mongodb/proto/messages  Messages_MessagesChatServer > mock_messages/messages_chat_server.go

echo "Generating mocks for Messages_MessagesChatClient"
mockgen github.com/percona/percona-backup-mongodb/proto/api  Api_GetClientsClient > mock_api/Api_getclients_client.go

echo "Generating mocks for MessagesClient"
mockgen github.com/percona/percona-backup-mongodb/proto/api  ApiClient > mock_api/api_client.go

echo "Generating mocks for MessagesServer"
mockgen github.com/percona/percona-backup-mongodb/proto/api  ApiServer > mock_api/api_server.go

echo "Generating mocks for Messages_MessagesChatServer"
mockgen github.com/percona/percona-backup-mongodb/proto/api  Api_GetClientsServer > mock_api/api_getclients_server.go
