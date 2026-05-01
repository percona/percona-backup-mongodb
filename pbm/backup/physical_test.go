package backup

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-backup-mongodb/pbm/log"
)

func TestBackupCursor(t *testing.T) {
	conn := TestEnv.Client.MongoClient()
	ctx := context.Background()
	l := log.DiscardEvent

	t.Run("create cursor", func(t *testing.T) {
		bc := NewBackupCursor(conn, l, bson.D{})
		cur, err := bc.create(ctx, cursorCreateRetries)
		if err != nil {
			t.Fatalf("create should succeed, got error: %v", err)
		}
		if cur == nil {
			t.Fatal("cursor should not be nil")
		}

		defer func() {
			if err := cur.Close(ctx); err != nil {
				t.Errorf("cursor should close without error, got: %v", err)
			}
		}()

		hasNext := cur.TryNext(ctx)
		if !hasNext {
			t.Error("cursor should have at least single document")
		}
	})

	t.Run("create cursor with error", func(t *testing.T) {
		tests := []struct {
			name       string
			errCode    int
			errNum     int
			retryNum   int
			wantErr    error
			wantCurNil bool
		}{
			// Error 50917: oplog rolled over backup cursor
			{
				name:       "50917/pass without error",
				errCode:    oplogRolledOverErrCode,
				errNum:     0,
				retryNum:   3,
				wantErr:    nil,
				wantCurNil: false,
			},
			{
				name:       "50917/pass after single retry",
				errCode:    oplogRolledOverErrCode,
				errNum:     1,
				retryNum:   3,
				wantErr:    nil,
				wantCurNil: false,
			},
			{
				name:       "50917/pass after last retry",
				errCode:    oplogRolledOverErrCode,
				errNum:     2,
				retryNum:   3,
				wantErr:    nil,
				wantCurNil: false,
			},
			{
				name:       "50917/fail for exact number of retries",
				errCode:    oplogRolledOverErrCode,
				errNum:     2,
				retryNum:   2,
				wantErr:    errTriesLimitExceeded,
				wantCurNil: true,
			},
			{
				name:       "50917/fail after all retries",
				errCode:    oplogRolledOverErrCode,
				errNum:     10,
				retryNum:   2,
				wantErr:    errTriesLimitExceeded,
				wantCurNil: true,
			},
			// Error 50915: BackupCursorOpenConflictWithCheckpoint
			{
				name:       "50915/pass without error",
				errCode:    openConflictWithCheckpointErrCode,
				errNum:     0,
				retryNum:   3,
				wantErr:    nil,
				wantCurNil: false,
			},
			{
				name:       "50915/pass after single retry",
				errCode:    openConflictWithCheckpointErrCode,
				errNum:     1,
				retryNum:   100,
				wantErr:    nil,
				wantCurNil: false,
			},
			{
				name:       "50915/pass after last retry",
				errCode:    openConflictWithCheckpointErrCode,
				errNum:     3,
				retryNum:   4,
				wantErr:    nil,
				wantCurNil: false,
			},
			{
				name:       "50915/fail for exact number of retries",
				errCode:    openConflictWithCheckpointErrCode,
				errNum:     2,
				retryNum:   2,
				wantErr:    errTriesLimitExceeded,
				wantCurNil: true,
			},
			{
				name:       "50915/fail after all retries",
				errCode:    openConflictWithCheckpointErrCode,
				errNum:     100,
				retryNum:   2,
				wantErr:    errTriesLimitExceeded,
				wantCurNil: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				setBcpCurErr(t, conn, tt.errCode, tt.errNum)
				defer func() {
					disableBcpCurErr(t, conn)
				}()

				bc := NewBackupCursor(conn, l, bson.D{})
				cur, err := bc.create(ctx, tt.retryNum)

				if err != tt.wantErr {
					t.Fatalf("create error mismatch: want=%v, got=%v", tt.wantErr, err)
				}

				if (cur == nil) != tt.wantCurNil {
					if tt.wantCurNil {
						t.Fatal("cursor should be nil")
					} else {
						t.Fatal("cursor should not be nil")
					}
				}

				if cur != nil {
					defer func() {
						if err := cur.Close(ctx); err != nil {
							t.Errorf("cursor should close without error, got: %v", err)
						}
					}()

					hasNext := cur.TryNext(ctx)
					if !hasNext {
						t.Error("cursor should have at least single document")
					}
				}
			})
		}
	})
}

func setBcpCurErr(t *testing.T, m *mongo.Client, errCode, times int) {
	t.Helper()

	err := m.Database("admin").RunCommand(context.Background(), bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", bson.D{
			{"skip", 0},
			{"times", times},
		}},
		{"data", bson.D{
			{"errorCode", errCode},
			{"failCommands", bson.A{"aggregate"}},
		}},
	}).Err()
	if err != nil {
		t.Fatalf("configureFailPoint set err: %v", err)
	}
}

func disableBcpCurErr(t *testing.T, m *mongo.Client) {
	t.Helper()

	err := m.Database("admin").RunCommand(context.Background(), bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", "off"},
	}).Err()
	if err != nil {
		t.Fatalf("configureFailPoint disable err: %v", err)
	}
}
