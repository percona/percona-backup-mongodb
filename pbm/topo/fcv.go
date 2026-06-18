package topo

import (
	"context"
	"net/url"
	"strings"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

// GetClusterFCV returns featureCompatibilityVersion for the cluster.
//
// MongoDB doesn't define getParameter featureCompatibilityVersion on mongos,
// so when connected to mongos this function temporarily connects to the config
// server replica set and reads FCV from there.
func GetClusterFCV(ctx context.Context, conn connect.Client) (string, error) {
	fcv, fcvErr := version.GetFCV(ctx, conn.MongoClient())
	if fcvErr == nil {
		return fcv, nil
	}

	info, err := GetNodeInfo(ctx, conn.MongoClient())
	if err != nil {
		return "", fcvErr
	}

	if !info.IsMongos() {
		return "", fcvErr
	}

	shards, err := getShardMapImpl(ctx, conn.MongoClient())
	if err != nil {
		return "", errors.Wrap(err, "get shard map")
	}

	var csrsHost string
	for _, shard := range shards {
		if shard.ID == "config" {
			csrsHost = shard.Host
			break
		}
	}
	if csrsHost == "" {
		return "", errors.New("config server is not found in shard map")
	}

	uri, err := clusterFCVConnURI(conn.MongoOptions().GetURI(), csrsHost)
	if err != nil {
		return "", err
	}

	client, err := connect.MongoConnect(ctx, uri, connect.AppName("pbm-fcv"), connect.NoRS())
	if err != nil {
		return "", errors.Wrap(err, "connect to config server")
	}
	defer func() { _ = client.Disconnect(context.Background()) }()

	fcv, err = version.GetFCV(ctx, client)
	if err != nil {
		return "", err
	}

	return fcv, nil
}

func clusterFCVConnURI(baseURI, replsetHost string) (string, error) {
	if baseURI == "" {
		return "", errors.New("original MongoDB URI is not available")
	}

	_, hosts, ok := strings.Cut(replsetHost, "/")
	if !ok || hosts == "" {
		return "", errors.Errorf("invalid config server connection URI %q", replsetHost)
	}

	uri, err := url.Parse(baseURI)
	if err != nil {
		return "", errors.Wrap(err, "parse mongo-uri")
	}

	query := uri.Query()
	query.Del("replicaSet")
	uri.RawQuery = query.Encode()
	uri.Host = hosts

	return uri.String(), nil
}
