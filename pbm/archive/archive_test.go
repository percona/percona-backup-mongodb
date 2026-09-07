package archive

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	mtarchive "github.com/mongodb/mongo-tools/common/archive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestComposeNamespaceHeaders(t *testing.T) {
	doc, err := bson.Marshal(bson.D{
		{"_id", 1},
		{"data", bson.Binary{Subtype: bson.TypeBinaryColumn, Data: []byte("compressed column")}},
	})
	require.NoError(t, err)

	tests := []struct {
		name           string
		serverVersion  string
		collectionType string
		body           []byte
		wantCollection string
	}{
		{
			name:           "MongoDB 8.0 time series",
			serverVersion:  "8.0.0",
			collectionType: "timeseries",
			body:           doc,
			wantCollection: "system.buckets.ts",
		},
		{
			name:           "MongoDB 8.2 time series",
			serverVersion:  "8.2.0",
			collectionType: "timeseries",
			body:           doc,
			wantCollection: "system.buckets.ts",
		},
		{
			name:           "MongoDB 8.3 time series",
			serverVersion:  "8.3.0",
			collectionType: "timeseries",
			body:           doc,
			wantCollection: "ts",
		},
		{
			name:           "MongoDB 8.3 empty time series",
			serverVersion:  "8.3.0",
			collectionType: "timeseries",
			wantCollection: "ts",
		},
		{
			name:           "regular collection",
			serverVersion:  "8.3.0",
			collectionType: "collection",
			body:           doc,
			wantCollection: "ts",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			meta := &archiveMeta{
				Header: &mtarchive.Header{
					ConcurrentCollections: 1,
					FormatVersion:         "0.1",
					ServerVersion:         test.serverVersion,
					ToolVersion:           "test",
				},
				Namespaces: []*Namespace{{
					CollectionMetadata: &mtarchive.CollectionMetadata{
						Database:   "test",
						Collection: "ts",
						Size:       len(test.body),
						Type:       test.collectionType,
					},
					CRC:  42,
					Size: int64(len(test.body)),
				}},
			}

			metadata, err := bson.MarshalExtJSON(meta, true, true)
			require.NoError(t, err)

			files := map[string][]byte{MetaFile: metadata}
			if len(test.body) != 0 {
				files["test.ts"] = test.body
			}
			newReader := func(name string) (io.ReadCloser, error) {
				data, ok := files[name]
				if !ok {
					return nil, fmt.Errorf("file %q not found", name)
				}
				return io.NopCloser(bytes.NewReader(data)), nil
			}

			var buf bytes.Buffer
			require.NoError(t, Compose(&buf, newReader, DefaultNSFilter, 1))

			reader := bytes.NewReader(buf.Bytes())
			prelude := &mtarchive.Prelude{}
			require.NoError(t, prelude.Read(reader))
			assert.Equal(t, test.serverVersion, prelude.Header.ServerVersion)

			collector := &namespaceCollector{}
			parser := mtarchive.Parser{In: reader}
			require.NoError(t, parser.ReadAllBlocks(collector))

			wantHeaders := 2
			if len(test.body) == 0 {
				wantHeaders = 1
			}
			require.Len(t, collector.headers, wantHeaders)
			for i, header := range collector.headers {
				assert.Equal(t, "test", header.Database, "header %d database", i)
				assert.Equal(t, test.wantCollection, header.Collection, "header %d collection", i)
				wantEOF := i == wantHeaders-1
				assert.Equal(t, wantEOF, header.EOF, "header %d EOF", i)
			}

			if len(test.body) == 0 {
				assert.Empty(t, collector.bodies)
				return
			}
			require.Len(t, collector.bodies, 1)
			assert.Equal(t, test.body, collector.bodies[0])
		})
	}
}

type namespaceCollector struct {
	headers []mtarchive.NamespaceHeader
	bodies  [][]byte
}

func (c *namespaceCollector) HeaderBSON(data []byte) error {
	header := mtarchive.NamespaceHeader{}
	if err := bson.Unmarshal(data, &header); err != nil {
		return err
	}
	c.headers = append(c.headers, header)
	return nil
}

func (c *namespaceCollector) BodyBSON(data []byte) error {
	c.bodies = append(c.bodies, bytes.Clone(data))
	return nil
}

func (*namespaceCollector) End() error {
	return nil
}
