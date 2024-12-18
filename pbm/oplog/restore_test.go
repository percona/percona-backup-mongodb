package oplog

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/idx"
	"github.com/mongodb/mongo-tools/mongorestore/ns"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm/snapshot"
)

func newOplogRestoreTest(mdb mDBCl) *OplogRestore {
	noUUID, _ := ns.NewMatcher(dontPreserveUUID)
	matcher, _ := ns.NewMatcher(append(snapshot.ExcludeFromRestore, excludeFromOplog...))
	return &OplogRestore{
		mdb:             mdb,
		ver:             &db.Version{7, 0, 0},
		excludeNS:       matcher,
		noUUIDns:        noUUID,
		preserveUUIDopt: true,
		preserveUUID:    true,
		indexCatalog:    idx.NewIndexCatalog(),
		filter:          DefaultOpFilter,
	}
}

type mdbTestClient struct {
}

func (d *mdbTestClient) getUUIDForNS(_ context.Context, _ string) (primitive.Binary, error) {
	return primitive.Binary{Subtype: 0x00, Data: []byte{0x01, 0x02, 0x03}}, nil
}

func (d *mdbTestClient) ensureCollExists(_ string) error {
	return nil
}

func (d *mdbTestClient) applyOps(entries []interface{}) error {
	return nil
}

func TestIsOpForCloning(t *testing.T) {
	oRestore := newOplogRestoreTest(&mdbTestClient{})
	oRestore.SetCloneNS(context.Background(), snapshot.CloneNS{FromNS: "mydb.cloningFrom", ToNS: "mydb.cloningTo"})

	testCases := []struct {
		desc         string
		entry        *db.Oplog
		isForCloning bool
	}{
		// i op
		{
			desc:         "insert op for cloning ",
			entry:        createInsertOp(t, "mydb.cloningFrom"),
			isForCloning: true,
		},
		{
			desc:         "insert op, collection not for cloning",
			entry:        createInsertOp(t, "mydb.x"),
			isForCloning: false,
		},
		{
			desc:         "insert op, db not for cloning",
			entry:        createInsertOp(t, "x.cloningFrom"),
			isForCloning: false,
		},

		// u op
		{
			desc:         "update op for cloning ",
			entry:        createUpdateOp(t, "mydb.cloningFrom"),
			isForCloning: true,
		},
		{
			desc:         "update op, collection not for cloning",
			entry:        createUpdateOp(t, "mydb.x"),
			isForCloning: false,
		},
		{
			desc:         "update op, db not for cloning",
			entry:        createUpdateOp(t, "x.cloningFrom"),
			isForCloning: false,
		},

		// d op
		{
			desc:         "delete op for cloning ",
			entry:        createDeleteOp(t, "mydb.cloningFrom"),
			isForCloning: true,
		},
		{
			desc:         "delete op, collection not for cloning",
			entry:        createDeleteOp(t, "mydb.x"),
			isForCloning: false,
		},
		{
			desc:         "delete op, db not for cloning",
			entry:        createDeleteOp(t, "x.cloningFrom"),
			isForCloning: false,
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			res := oRestore.isOpForCloning(tC.entry)
			if res != tC.isForCloning {
				t.Errorf("%s: for entry: %+v isOpForCloning is: %t, but it should be opposite",
					tC.desc, tC.entry, tC.isForCloning)

			}
		})
	}
}

func TestApply(t *testing.T) {
	oRestore := newOplogRestoreTest(&mdbTestClient{})
	oRestore.SetCloneNS(context.Background(), snapshot.CloneNS{FromNS: "mydb.c5", ToNS: "mydb.c5"})

	fr := useTestFile(t, "oplog_test.json")

	lts, err := oRestore.Apply(fr)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(lts)
}

func useTestFile(t *testing.T, testFileName string) io.ReadCloser {
	t.Helper()

	jsonData, err := os.ReadFile(filepath.Join("./testdata", testFileName))
	if err != nil {
		t.Fatalf("failed to read test json file: filename=%s, err=%v", testFileName, err)
	}

	var jsonDocs []map[string]interface{}
	err = bson.UnmarshalExtJSON(jsonData, false, &jsonDocs)
	if err != nil {
		t.Fatalf("failed to parse test json array: filename=%s, err=%v", testFileName, err)
	}

	b := &bytes.Buffer{}
	for _, jsonDoc := range jsonDocs {
		bsonDoc, err := bson.Marshal(jsonDoc)
		if err != nil {
			t.Fatalf("failed to marshal json to bson: %v", err)
		}
		_, err = b.Write(bsonDoc)
		if err != nil {
			t.Fatalf("Failed to write BSON: %v", err)
		}
	}

	return io.NopCloser(b)
}

func createInsertOp(t *testing.T, ns string) *db.Oplog {
	t.Helper()
	iOpJSON := `
		{
		  "lsid": {
			"id": {
			  "$binary": {
				"base64": "YYbkO7kpRt6xFqJqIh+h9g==",
				"subType": "04"
			  }
			},
			"uid": {
			  "$binary": {
				"base64": "8L/kOoqHkvDRIRJTrmrrO3wwOr+ToO8WLvmn15Ql7G0=",
				"subType": "00"
			  }
			}
		  },
		  "txnNumber": {
			"$numberLong": "9"
		  },
		  "op": "i",
		  "ns": "db.coll",
		  "ui": {
			"$binary": {
			  "base64": "v+mHa8niRBKG7Z+uqJGARQ==",
			  "subType": "04"
			}
		  },
		  "o": {
			"_id": {
			  "$oid": "6747008178d82a2b1134a2b8"
			},
			"d": {
			  "$numberInt": "6"
			},
			"desc": "doc-6"
		  },
		  "o2": {
			"_id": {
			  "$oid": "6747008178d82a2b1134a2b8"
			}
		  },
		  "stmtId": {
			"$numberInt": "0"
		  },
		  "ts": {
			"$timestamp": {
			  "t": 1732706433,
			  "i": 1
			}
		  },
		  "t": {
			"$numberLong": "2"
		  },
		  "v": {
			"$numberLong": "2"
		  },
		  "wall": {
			"$date": {
			  "$numberLong": "1732706433987"
			}
		  },
		  "prevOpTime": {
			"ts": {
			  "$timestamp": {
				"t": 0,
				"i": 0
			  }
			},
			"t": {
			  "$numberLong": "-1"
			}
		  }
		}`

	return replaceNsWithinOpEntry(t, iOpJSON, ns)
}

func createUpdateOp(t *testing.T, ns string) *db.Oplog {
	t.Helper()

	uOpJSON := `
		{
		  "lsid": {
			"id": {
			  "$binary": {
				"base64": "HxXre7SSRxe8eq+OjOQOhw==",
				"subType": "04"
			  }
			},
			"uid": {
			  "$binary": {
				"base64": "Bh/Anp+//gSHltMgOtOX+7sunrF/VwW+VDdA3fRANl0=",
				"subType": "00"
			  }
			}
		  },
		  "txnNumber": {
			"$numberLong": "5"
		  },
		  "op": "u",
		  "ns": "db.coll",
		  "ui": {
			"$binary": {
			  "base64": "f774YvKERIKXSJVH+xCtPw==",
			  "subType": "04"
			}
		  },
		  "o": {
			"$v": 2,
			"diff": {
			  "i": {
				"city": "split"
			  }
			}
		  },
		  "o2": {
			"_id": {
			  "$oid": "6728e3fcedfb509c06f01307"
			}
		  },
		  "stmtId": 0,
		  "ts": {
			"$timestamp": {
			  "t": 1730733212,
			  "i": 1
			}
		  },
		  "t": {
			"$numberLong": "7"
		  },
		  "v": {
			"$numberLong": "2"
		  },
		  "wall": {
			"$date": "2024-11-04T15:13:32.123Z"
		  },
		  "prevOpTime": {
			"ts": {
			  "$timestamp": {
				"t": 0,
				"i": 0
			  }
			},
			"t": {
			  "$numberLong": "-1"
			}
		  }
		}`
	return replaceNsWithinOpEntry(t, uOpJSON, ns)
}

func createDeleteOp(t *testing.T, ns string) *db.Oplog {
	t.Helper()

	dOpJSON := `
		{
		  "lsid": {
			"id": {
			  "$binary": {
				"base64": "HxXre7SSRxe8eq+OjOQOhw==",
				"subType": "04"
			  }
			},
			"uid": {
			  "$binary": {
				"base64": "Bh/Anp+//gSHltMgOtOX+7sunrF/VwW+VDdA3fRANl0=",
				"subType": "00"
			  }
			}
		  },
		  "txnNumber": {
			"$numberLong": "6"
		  },
		  "op": "d",
		  "ns": "db.coll",
		  "ui": {
			"$binary": {
			  "base64": "f774YvKERIKXSJVH+xCtPw==",
			  "subType": "04"
			}
		  },
		  "o": {
			"_id": {
			  "$oid": "6728e3fcedfb509c06f01307"
			}
		  },
		  "stmtId": 0,
		  "ts": {
			"$timestamp": {
			  "t": 1730733256,
			  "i": 1
			}
		  },
		  "t": {
			"$numberLong": "7"
		  },
		  "v": {
			"$numberLong": "2"
		  },
		  "wall": {
			"$date": "2024-11-04T15:14:16.626Z"
		  },
		  "prevOpTime": {
			"ts": {
			  "$timestamp": {
				"t": 0,
				"i": 0
			  }
			},
			"t": {
			  "$numberLong": "-1"
			}
		  }
		}`

	return replaceNsWithinOpEntry(t, dOpJSON, ns)
}

func replaceNsWithinOpEntry(t *testing.T, jsonEntry, ns string) *db.Oplog {
	t.Helper()

	oe := db.Oplog{}
	err := bson.UnmarshalExtJSON([]byte(jsonEntry), false, &oe)
	if err != nil {
		t.Errorf("err while unmarshal from json: %v", err)
	}

	if ns != "" {
		oe.Namespace = ns
	}
	return &oe
}
