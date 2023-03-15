package cli

import (
	"reflect"
	"testing"

	"github.com/percona/percona-backup-mongodb/v2/pbm"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func Test_splitByBaseSnapshot(t *testing.T) {
	tl := pbm.Timeline{Start: 3, End: 7}

	t.Run("lastWrite is nil", func(t *testing.T) {
		got := splitByBaseSnapshot(nil, tl)

		want := []pitrRange{
			{Range: tl, NoBaseSnapshot: true},
		}

		check(t, got, want)
	})

	t.Run("lastWrite > tl.End", func(t *testing.T) {
		lastWrite := &primitive.Timestamp{T: tl.End + 1}
		got := splitByBaseSnapshot(lastWrite, tl)

		want := []pitrRange{
			{Range: tl, NoBaseSnapshot: true},
		}

		check(t, got, want)
	})

	t.Run("lastWrite = tl.End", func(t *testing.T) {
		lastWrite := &primitive.Timestamp{T: tl.End}
		got := splitByBaseSnapshot(lastWrite, tl)

		want := []pitrRange{
			{Range: tl, NoBaseSnapshot: true},
		}

		check(t, got, want)
	})

	t.Run("lastWrite < tl.Start", func(t *testing.T) {
		lastWrite := &primitive.Timestamp{T: tl.Start - 1}
		got := splitByBaseSnapshot(lastWrite, tl)

		want := []pitrRange{
			{Range: tl, NoBaseSnapshot: true},
		}

		check(t, got, want)
	})

	t.Run("lastWrite = tl.Start", func(t *testing.T) {
		lastWrite := &primitive.Timestamp{T: tl.Start}
		got := splitByBaseSnapshot(lastWrite, tl)

		want := []pitrRange{
			{
				Range: pbm.Timeline{
					Start: lastWrite.T + 1,
					End:   tl.End,
				},
				NoBaseSnapshot: false,
			},
		}

		check(t, got, want)
	})

	t.Run("tl.Start < lastWrite < tl.End", func(t *testing.T) {
		lastWrite := &primitive.Timestamp{T: 5}
		got := splitByBaseSnapshot(lastWrite, tl)

		want := []pitrRange{
			{
				Range: pbm.Timeline{
					Start: tl.Start,
					End:   lastWrite.T,
				},
				NoBaseSnapshot: true,
			},
			{
				Range: pbm.Timeline{
					Start: lastWrite.T + 1,
					End:   tl.End,
				},
				NoBaseSnapshot: false,
			},
		}

		check(t, got, want)
	})
}

func check(t *testing.T, got, want []pitrRange) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}
}
