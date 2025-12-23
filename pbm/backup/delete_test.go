package backup

import (
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	bsonv2 "go.mongodb.org/mongo-driver/v2/bson"
)

type chunk struct {
	From     time.Time
	To       time.Time
	Expected bool
}

func NewRelativeTime(t time.Time, unit time.Duration) func(int) time.Time {
	return func(delta int) time.Time {
		return t.Add(-1 * unit * time.Duration(delta))
	}
}

type TestStorage struct {
	stgType storage.Type
	deletes []string
}

func (ts *TestStorage) AssertDeleted(t *testing.T, name string) {
	t.Helper()
	assert.Contains(t, ts.deletes, name)
}

func (ts *TestStorage) AssertDeleteCalls(t *testing.T, count int) {
	t.Helper()
	assert.Len(t, ts.deletes, count, "expected %d delete calls, got %d", count, len(ts.deletes))
}

func (ts *TestStorage) Type() storage.Type {
	return ts.stgType
}

func (ts *TestStorage) Save(name string, _ io.Reader, _ ...storage.Option) error {
	return nil
}

func (ts *TestStorage) SourceReader(name string) (io.ReadCloser, error) {
	return nil, errors.New("not implemented")
}

func (ts *TestStorage) FileStat(name string) (storage.FileInfo, error) {
	return storage.FileInfo{Name: name, Size: 0}, nil
}

func (ts *TestStorage) List(prefix, suffix string) ([]storage.FileInfo, error) {
	return nil, nil
}

func (ts *TestStorage) Delete(name string) error {
	ts.deletes = append(ts.deletes, name)
	return nil
}

func (ts *TestStorage) Copy(src, dst string) error {
	return nil
}

func (ts *TestStorage) DownloadStat() storage.DownloadStat {
	return storage.DownloadStat{}
}

func NewTestStorage(stgType storage.Type) *TestStorage {
	return &TestStorage{stgType: stgType}
}

func TestIsRequiredForOplogSlicing(t *testing.T) {
	tm := NewRelativeTime(time.Now(), time.Minute)

	tests := []struct {
		name        string
		lwt         time.Time
		baseLWT     time.Time
		backups     []bcp
		chunks      []chunk
		pitrEnabled [2]bool
		expected    bool
	}{
		{
			name:        "more recent snapshot exists",
			pitrEnabled: [2]bool{true, false},
			lwt:         tm(20),
			backups:     []bcp{{LWT: tm(25)}, {LWT: tm(20)}, {LWT: tm(15)}},
			chunks: []chunk{
				{From: tm(14), To: tm(5)},
			},
			expected: false,
		},
		{
			name:        "gap in pitr timeline",
			pitrEnabled: [2]bool{true, false},
			lwt:         tm(20),
			backups:     []bcp{{LWT: tm(30)}, {LWT: tm(20)}},
			chunks: []chunk{
				{From: tm(29), To: tm(25)},
				{From: tm(22), To: tm(20)},
			},
			expected: false,
		},
		{
			name:        "continuous pitr timeline (single chunk)",
			pitrEnabled: [2]bool{true, false},
			lwt:         tm(20),
			backups:     []bcp{{LWT: tm(30)}, {LWT: tm(20)}},
			chunks: []chunk{
				{From: tm(29), To: tm(20)},
			},
			expected: true,
		},
		{
			name:        "continuous pitr timeline (multiple chunks)",
			pitrEnabled: [2]bool{true, false},
			lwt:         tm(20),
			backups:     []bcp{{LWT: tm(30)}, {LWT: tm(20)}},
			chunks: []chunk{
				{From: tm(29), To: tm(25)},
				{From: tm(25), To: tm(20)},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{
				PITR: &config.PITRConf{
					Enabled:   tt.pitrEnabled[0],
					OplogOnly: tt.pitrEnabled[1],
				},
			}
			TestEnv.ResetWithConfig(t, cfg)
			insertTestBackupsStorage(t, TestEnv, TestEnv.PbmStorage, tt.backups)
			insertTestChunks(t, TestEnv, tt.chunks)

			lwtBson := bsonv2.Timestamp{T: uint32(tt.lwt.Unix())}
			baseLWTBson := bsonv2.Timestamp{T: uint32(tt.baseLWT.Unix())}
			if baseLWTBson.IsZero() {
				baseLWTBson = bsonv2.Timestamp{}
			}

			actual, err := isRequiredForOplogSlicing(t.Context(), TestEnv.Client, lwtBson, baseLWTBson)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestDeleteBackupData(t *testing.T) {
	tests := []struct {
		name   string
		backup bcp
		stg    *TestStorage
	}{
		{
			name:   "metadata and FS storage",
			backup: bcp{Name: "my-test-bcp", LWT: time.Now()},
			stg:    NewTestStorage(storage.Filesystem),
		},
		{
			name:   "metadata and object storage",
			backup: bcp{Name: "my-test-bcp", LWT: time.Now()},
			stg:    NewTestStorage(storage.S3),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			TestEnv.Reset(t)

			b := insertTestBcpMeta(t, TestEnv, TestEnv.PbmStorage, tt.backup)

			err := DeleteBackupData(t.Context(), TestEnv.Client, tt.stg, b.Name)
			assert.NoError(t, err)
			assertBackupDeleted(t, TestEnv.Client, tt.stg, b.Name)
		})
	}
}

func TestListDeleteBackups(t *testing.T) {
	tm := NewRelativeTime(time.Now(), time.Minute)

	tests := []struct {
		name    string
		before  time.Time
		bcpType defs.BackupType
		backups map[string][]bcp
	}{
		{
			name:    "no older backups",
			before:  tm(30),
			bcpType: defs.LogicalBackup,
			backups: map[string][]bcp{
				"": {
					{LWT: tm(20), BcpType: defs.LogicalBackup},
				},
				"aaa": {
					{LWT: tm(10), BcpType: defs.LogicalBackup},
				},
			},
		},
		{
			name:    "no older physical backups in profiles",
			before:  tm(15),
			bcpType: defs.PhysicalBackup,
			backups: map[string][]bcp{
				"": {
					{LWT: tm(5), BcpType: defs.LogicalBackup},
					{LWT: tm(10), BcpType: defs.PhysicalBackup},
					{LWT: tm(30), BcpType: defs.PhysicalBackup, Expected: true},
				},
				"aaa": {
					{LWT: tm(5), BcpType: defs.PhysicalBackup},
					{LWT: tm(10), BcpType: defs.PhysicalBackup},
					{LWT: tm(30), BcpType: defs.LogicalBackup},
				},
			},
		},
		{
			name:    "no older physical backups in default",
			before:  tm(10),
			bcpType: defs.PhysicalBackup,
			backups: map[string][]bcp{
				"": {
					{LWT: tm(5), BcpType: defs.PhysicalBackup},
					{LWT: tm(10), BcpType: defs.PhysicalBackup},
					{LWT: tm(30), BcpType: defs.LogicalBackup},
				},
				"aaa": {
					{LWT: tm(5), BcpType: defs.LogicalBackup},
					{LWT: tm(10), BcpType: defs.PhysicalBackup},
					{LWT: tm(30), BcpType: defs.PhysicalBackup, Expected: true},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			TestEnv.Reset(t)

			storages := stgsFromTestBackups(t, tt.backups)
			expected := insertTestBackups(t, TestEnv, storages, tt.backups)
			before := bsonv2.Timestamp{T: uint32(tt.before.Unix())}

			for profile := range tt.backups {
				bcps, err := ListDeleteBackupBefore(t.Context(), TestEnv.Client, before, tt.bcpType, profile)

				assert.NoError(t, err)
				assertBackupList(t, expected[profile], bcps, profile)
			}
		})
	}
}

func TestMakeCleanupInfo(t *testing.T) {
	tm := NewRelativeTime(time.Now(), time.Minute)
	type testCase struct {
		name    string
		before  time.Time
		chunks  []chunk
		backups map[string][]bcp
		pitr    bool
	}

	tests := []testCase{
		{
			name:   "chunks only in default profile; backups only in profiles",
			before: tm(25),
			chunks: []chunk{
				{From: tm(60), To: tm(50), Expected: true},
				{From: tm(50), To: tm(20), Expected: true},
				{From: tm(20), To: tm(10)},
			},
			backups: map[string][]bcp{
				"aaa": {
					{LWT: tm(30), Expected: true},
					{LWT: tm(15)},
				},
				"bbb": {
					{LWT: tm(26), Expected: true},
					{LWT: tm(5)},
				},
			},
		},
		{
			name:   "no older chunks; some older backups in a profile",
			before: tm(10),
			chunks: []chunk{
				{From: tm(9), To: tm(5)},
				{From: tm(5), To: tm(1)},
			},
			backups: map[string][]bcp{
				"aaa": {
					{LWT: tm(20), Expected: true},
					{LWT: tm(8)},
				},
			},
		},
		{
			name:   "older backups only in default profile",
			before: tm(10),
			chunks: []chunk{
				{From: tm(60), To: tm(40), Expected: true},
				{From: tm(40), To: tm(20), Expected: true},
				{From: tm(10), To: tm(5)},
			},
			backups: map[string][]bcp{
				"": {
					{LWT: tm(25), Expected: true},
					{LWT: tm(6)},
				},
				"aaa": {
					{LWT: tm(9)},
					{LWT: tm(2)},
				},
			},
		},
		{
			name:   "older backups in default and other profiles alike",
			before: tm(10),
			chunks: []chunk{
				{From: tm(80), To: tm(50), Expected: true},
				{From: tm(50), To: tm(30), Expected: true},
				{From: tm(25), To: tm(20), Expected: true},
				{From: tm(9), To: tm(5)},
			},
			backups: map[string][]bcp{
				"": {
					{LWT: tm(30), Expected: true},
					{LWT: tm(8)},
				},
				"aaa": {
					{LWT: tm(25), Expected: true},
					{LWT: tm(5)},
				},
				"bbb": {
					{LWT: tm(40), Expected: true},
					{LWT: tm(11), Expected: true},
				},
			},
		},
		{
			name:   "backup is a pitr base",
			pitr:   true,
			before: tm(25),
			chunks: []chunk{
				{From: tm(29), To: tm(25), Expected: false},
				{From: tm(25), To: tm(20), Expected: false},
			},
			backups: map[string][]bcp{
				"": {
					{LWT: tm(30), Expected: false},
				},
			},
		},
		{
			name:   "fake-base in profile is deleted",
			pitr:   true,
			before: tm(25),
			chunks: []chunk{
				{From: tm(29), To: tm(25), Expected: false},
				{From: tm(25), To: tm(20), Expected: false},
			},
			backups: map[string][]bcp{
				"": {
					{LWT: tm(30), Expected: false},
				},
				"aaa": {
					{LWT: tm(30), Expected: true},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{}
			if tt.pitr {
				cfg.PITR = &config.PITRConf{
					Enabled:   true,
					OplogOnly: false,
				}
			}
			TestEnv.ResetWithConfig(t, cfg)

			storages := stgsFromTestBackups(t, tt.backups)
			expectedChunks := insertTestChunks(t, TestEnv, tt.chunks)
			expectedBackupNames := insertTestBackups(t, TestEnv, storages, tt.backups)
			before := bsonv2.Timestamp{T: uint32(tt.before.Unix())}

			for profile := range tt.backups {
				info, err := MakeCleanupInfo(t.Context(), TestEnv.Client, before, profile)
				assert.NoError(t, err)

				assertBackupList(t, expectedBackupNames[profile], info.Backups, profile)
				assertChunkList(t, expectedChunks, info.Chunks, profile)
			}
		})
	}
}

func TestListChunksBefore(t *testing.T) {
	tm := NewRelativeTime(time.Now(), time.Minute)

	tests := []struct {
		name   string
		before time.Time
		chunks []chunk
	}{
		{
			name:   "no older chunks",
			before: tm(60),
			chunks: []chunk{
				{From: tm(50), To: tm(20)},
				{From: tm(20), To: tm(10)},
			},
		},
		{
			name:   "all older chunks",
			before: tm(5),
			chunks: []chunk{
				{From: tm(50), To: tm(20), Expected: true},
				{From: tm(20), To: tm(10), Expected: true},
			},
		},
		{
			name:   "some older chunks",
			before: tm(25),
			chunks: []chunk{
				{From: tm(60), To: tm(50), Expected: true},
				{From: tm(20), To: tm(10)},
			},
		},
		{
			name:   "older chunk overlapping with before",
			before: tm(25),
			chunks: []chunk{
				{From: tm(40), To: tm(10), Expected: true}, // overlaps with before
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			TestEnv.Reset(t)

			before := bsonv2.Timestamp{T: uint32(tt.before.Unix())}
			expected := insertTestChunks(t, TestEnv, tt.chunks)

			chunks, err := listChunksBefore(t.Context(), TestEnv.Client, before)
			assert.NoError(t, err)

			assertChunkList(t, expected, chunks, "")
		})
	}

}

func TestListBackupsBefore(t *testing.T) {
	tm := NewRelativeTime(time.Now(), time.Minute)

	tests := []struct {
		name    string
		before  time.Time
		backups map[string][]bcp
	}{
		{
			name:   "no older backups",
			before: tm(30),
			backups: map[string][]bcp{
				"": {
					{LWT: tm(20)},
				},
				"aaa": {
					{LWT: tm(10)},
				},
			},
		},
		{
			name:   "no older backups in profiles",
			before: tm(10),
			backups: map[string][]bcp{
				"": {
					{LWT: tm(5)},
					{LWT: tm(30), Expected: true},
				},
				"aaa": {
					{LWT: tm(5)},
				},
			},
		},
		{
			name:   "no older backups in default",
			before: tm(10),
			backups: map[string][]bcp{
				"": {
					{LWT: tm(5)},
				},
				"aaa": {
					{LWT: tm(20), Expected: true},
				},
			},
		},
		{
			name:   "older backups in default and single profile",
			before: tm(10),
			backups: map[string][]bcp{
				"": {
					{LWT: tm(5)},
					{LWT: tm(20), Expected: true},
				},
				"aaa": {
					{LWT: tm(5)},
					{LWT: tm(15), Expected: true},
				},
			},
		},
		{
			name:   "older backups in default and profiles",
			before: tm(10),
			backups: map[string][]bcp{
				"": {
					{LWT: tm(5)},
					{LWT: tm(20), Expected: true},
				},
				"aaa": {
					{LWT: tm(5)},
					{LWT: tm(15), Expected: true},
				},
				"bbb": {
					{LWT: tm(10)},
					{LWT: tm(20), Expected: true},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			TestEnv.Reset(t)

			storages := stgsFromTestBackups(t, tt.backups)
			expected := insertTestBackups(t, TestEnv, storages, tt.backups)
			before := bsonv2.Timestamp{T: uint32(tt.before.Unix())}

			for profile := range tt.backups {
				bcps, err := listBackupsBefore(t.Context(), TestEnv.Client, before, profile)
				assert.NoError(t, err)
				assertBackupList(t, expected[profile], bcps, profile)
			}
		})
	}
}

func extractChunkStarts(chunks []oplog.OplogChunk) []uint32 {
	starts := make([]uint32, len(chunks))
	for i, c := range chunks {
		starts[i] = c.StartTS.T
	}
	return starts
}

func assertBackupDeleted(t *testing.T, client connect.Client, stg *TestStorage, name string) {
	t.Helper()
	count, err := client.BcpCollection().CountDocuments(t.Context(), bson.M{"name": name})
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count, "backup metadata %s should be deleted", name)

	if stg.Type() == storage.Filesystem {
		stg.AssertDeleteCalls(t, 2)
		stg.AssertDeleted(t, name)
		stg.AssertDeleted(t, name+defs.MetadataFileSuffix)
	} else {
		//  only metadata should be deleted as test storage doesn't list files
		stg.AssertDeleteCalls(t, 1)
	}
}

func assertChunkList(t *testing.T, expectedChunks []oplog.OplogChunk, actualChunks []oplog.OplogChunk, profile string) {
	t.Helper()

	if profile != "" {
		// chunks are stored only in the default profile
		assert.Empty(t, actualChunks, "expected no chunks for profile")
		return
	}

	assert.Len(t, actualChunks, len(expectedChunks))
	expectedStarts := extractChunkStarts(expectedChunks)
	actualStarts := extractChunkStarts(actualChunks)

	assert.ElementsMatchf(
		t,
		expectedStarts,
		actualStarts,
		"expected defaut profile to have chunks %v, got %v",
		expectedStarts,
		actualStarts,
	)
}

func extractBackupNames(backups []BackupMeta) []string {
	names := make([]string, len(backups))
	for i, b := range backups {
		names[i] = b.Name
	}
	return names
}

func assertBackupList(t *testing.T, expectedBackups []BackupMeta, actualBackups []BackupMeta, profile string) {
	t.Helper()

	assert.Len(t, actualBackups, len(expectedBackups))
	expectedNames := extractBackupNames(expectedBackups)
	actualNames := extractBackupNames(actualBackups)

	assert.ElementsMatchf(
		t,
		expectedNames,
		actualNames,
		"expected profile %q to have backups %v, got %v",
		profile,
		expectedNames,
		actualNames,
	)
}

func insertTestBackupsStorage(t *testing.T, env *TestEnvironment, stg Storage, bcps []bcp) []BackupMeta {
	inserted := make([]BackupMeta, len(bcps))
	for i, b := range bcps {
		if b.Name == "" {
			b.Name = fmt.Sprintf("backup-%s-%d", stg.Name, i)
		}
		inserted[i] = insertTestBcpMeta(t, env, stg, b)
	}
	return inserted
}

func insertTestBackups(
	t *testing.T,
	env *TestEnvironment,
	storages map[string]Storage,
	profileBcps map[string][]bcp,
) map[string][]BackupMeta {
	t.Helper()

	expected := make(map[string][]BackupMeta)
	for profile, bcps := range profileBcps {
		inserted := insertTestBackupsStorage(t, env, storages[profile], bcps)
		for i, bcp := range bcps {
			if bcp.Expected {
				expected[profile] = append(expected[profile], inserted[i])
			}
		}
	}
	return expected
}

func insertTestChunks(t *testing.T, env *TestEnvironment, chunks []chunk) []oplog.OplogChunk {
	t.Helper()

	var expected []oplog.OplogChunk
	for _, c := range chunks {
		inserted := insertTestChunkMeta(t, env, c)
		if c.Expected {
			expected = append(expected, inserted)
		}
	}
	return expected
}

func insertTestChunkMeta(t *testing.T, env *TestEnvironment, c chunk) oplog.OplogChunk {
	t.Helper()

	fromBson := bsonv2.Timestamp{T: uint32(c.From.Unix())}
	toBson := bsonv2.Timestamp{T: uint32(c.To.Unix())}
	compression := compress.CompressionTypeS2
	filename := oplog.FormatChunkFilepath(env.Brief.SetName, fromBson, toBson, compression)

	meta := oplog.OplogChunk{
		RS:          env.Brief.SetName,
		FName:       filename,
		Compression: compression,
		StartTS:     fromBson,
		EndTS:       toBson,
	}

	_, err := env.Client.PITRChunksCollection().InsertOne(t.Context(), meta)
	require.NoError(t, err)

	return meta
}
