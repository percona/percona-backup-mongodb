package pbm

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestPITRTimelines(t *testing.T) {
	chunks := []PITRChunk{
		{
			StartTS: primitive.Timestamp{1, 0},
			EndTS:   primitive.Timestamp{10, 0},
		},
		{
			StartTS: primitive.Timestamp{10, 0},
			EndTS:   primitive.Timestamp{20, 0},
		},
		{
			StartTS: primitive.Timestamp{30, 0},
			EndTS:   primitive.Timestamp{40, 0},
		},
		{
			StartTS: primitive.Timestamp{666, 0},
			EndTS:   primitive.Timestamp{777, 0},
		},
		{
			StartTS: primitive.Timestamp{777, 0},
			EndTS:   primitive.Timestamp{888, 0},
		},
		{
			StartTS: primitive.Timestamp{888, 0},
			EndTS:   primitive.Timestamp{1000, 0},
		},
	}

	tlines := []Timeline{
		{
			Start: 1,
			End:   20,
		},
		{
			Start: 30,
			End:   40,
		},
		{
			Start: 666,
			End:   1000,
		},
	}

	glines := gettimelines(chunks)
	if len(tlines) != len(glines) {
		t.Fatalf("wrong timelines, exepct [%d] %v, got [%d] %v", len(tlines), tlines, len(glines), glines)
	}
	for i, gl := range glines {
		if tlines[i] != gl {
			t.Errorf("wrong timeline %d, exepct %v, got %v", i, tlines[i], gl)
		}
	}
}

func TestPITRMergeTimelines(t *testing.T) {
	tlns := [][]Timeline{
		{
			{
				Start: 3,
				End:   11,
			},
			{
				Start: 14,
				End:   19,
			},
			{
				Start: 20,
				End:   30,
			},
			{
				Start: 42,
				End:   100500,
			},
		},

		{
			{
				Start: 2,
				End:   12,
			},
			{
				Start: 14,
				End:   20,
			},
			{
				Start: 20,
				End:   30,
			},
		},

		{
			{
				Start: 1,
				End:   5,
			},
			{
				Start: 13,
				End:   19,
			},
			{
				Start: 20,
				End:   30,
			},
		},
	}

	should := []Timeline{
		{
			Start: 3,
			End:   5,
		},
		{
			Start: 14,
			End:   19,
		},
		{
			Start: 20,
			End:   30,
		},
	}

	got := MergeTimelines(tlns...)
	if len(should) != len(got) {
		t.Fatalf("wrong timelines, exepct [%d] %v, got [%d] %v", len(should), should, len(got), got)
	}
	for i, gl := range got {
		if should[i] != gl {
			t.Errorf("wrong timeline %d, exepct %v, got %v", i, should[i], gl)
		}
	}
}
