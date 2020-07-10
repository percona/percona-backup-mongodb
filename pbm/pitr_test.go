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

	glines := gettlines(chunks)
	if len(tlines) != len(glines) {
		t.Fatalf("wrong timelines, exepct [%d] %v, got [%d] %v", len(tlines), tlines, len(glines), glines)
	}
	for i, gl := range glines {
		if tlines[i] != gl {
			t.Errorf("wrong timeline %d, exepct %v, got %v", i, tlines[i], gl)
		}
	}

}
