package agent

import (
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func Test_findMissedRanges(t *testing.T) {
	from := primitive.Timestamp{T: 5}
	till := primitive.Timestamp{T: 45}

	t.Run("empty", func(t *testing.T) {
		expect := []timerange{
			{from, till},
		}
		got := findChunkRanges(nil, from, till)

		if !reflect.DeepEqual(got, expect) {
			t.Errorf("expected %v, got %v", expect, got)
		}
	})

	t.Run("full", func(t *testing.T) {
		t.Run("exact", func(t *testing.T) {
			chunks := []pbm.OplogChunk{
				{StartTS: from, EndTS: till},
			}
			got := findChunkRanges(chunks, from, till)

			if len(got) != 0 {
				t.Errorf("expected empty, got %v", got)
			}
		})

		t.Run("within", func(t *testing.T) {
			chunks := []pbm.OplogChunk{
				{StartTS: primitive.Timestamp{T: from.T - 1}, EndTS: primitive.Timestamp{T: till.T + 1}},
			}
			got := findChunkRanges(chunks, from, till)

			if len(got) != 0 {
				t.Errorf("expected empty, got %v", got)
			}
		})

		t.Run("exact (multiple)", func(t *testing.T) {
			chunks := []pbm.OplogChunk{
				{StartTS: from, EndTS: primitive.Timestamp{T: 20}},
				{StartTS: primitive.Timestamp{T: 20}, EndTS: primitive.Timestamp{T: 30}},
				{StartTS: primitive.Timestamp{T: 30}, EndTS: till},
			}
			got := findChunkRanges(chunks, from, till)

			if len(got) != 0 {
				t.Errorf("expected empty, got %v", got)
			}
		})

		t.Run("within (multiple)", func(t *testing.T) {
			chunks := []pbm.OplogChunk{
				{StartTS: primitive.Timestamp{T: from.T - 1}, EndTS: primitive.Timestamp{T: 20}},
				{StartTS: primitive.Timestamp{T: 20}, EndTS: primitive.Timestamp{T: 30}},
				{StartTS: primitive.Timestamp{T: 30}, EndTS: primitive.Timestamp{T: till.T + 1}},
			}
			got := findChunkRanges(chunks, from, till)

			if len(got) != 0 {
				t.Errorf("expected empty, got %v", got)
			}
		})
	})

	t.Run("partial", func(t *testing.T) {
		chunks := []pbm.OplogChunk{
			{StartTS: primitive.Timestamp{T: 10}, EndTS: primitive.Timestamp{T: 20}},
			{StartTS: primitive.Timestamp{T: 30}, EndTS: primitive.Timestamp{T: 35}},
			{StartTS: primitive.Timestamp{T: 35}, EndTS: primitive.Timestamp{T: 40}},
			{StartTS: primitive.Timestamp{T: 45}, EndTS: primitive.Timestamp{T: 50}},
		}
		expect := []timerange{
			{primitive.Timestamp{T: 5}, primitive.Timestamp{T: 10}},
			{primitive.Timestamp{T: 20}, primitive.Timestamp{T: 30}},
			{primitive.Timestamp{T: 40}, primitive.Timestamp{T: 45}},
		}
		got := findChunkRanges(chunks, from, till)

		if !reflect.DeepEqual(got, expect) {
			t.Errorf("expected %v, got %v", expect, got)
		}
	})
}
