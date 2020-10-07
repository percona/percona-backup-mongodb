package pbm

import (
	"fmt"
	"strings"
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
	var tests = []struct {
		name   string
		tl     [][]Timeline
		expect []Timeline
	}{
		{
			name: "no match",
			tl: [][]Timeline{
				{
					{Start: 3, End: 6},
					{Start: 14, End: 19},
					{Start: 19, End: 42},
				},
				{
					{Start: 1, End: 3},
					{Start: 6, End: 14},
					{Start: 50, End: 55},
				},
				{
					{Start: 7, End: 10},
					{Start: 12, End: 19},
					{Start: 20, End: 26},
					{Start: 26, End: 60},
				},
			},
			expect: []Timeline{},
		},
		{
			name: "all match",
			tl: [][]Timeline{
				{
					{Start: 3, End: 6},
					{Start: 14, End: 19},
					{Start: 19, End: 42},
				},
				{
					{Start: 3, End: 6},
					{Start: 14, End: 19},
					{Start: 19, End: 42},
				},
				{
					{Start: 3, End: 6},
					{Start: 14, End: 19},
					{Start: 19, End: 42},
				},
				{
					{Start: 3, End: 6},
					{Start: 14, End: 19},
					{Start: 19, End: 42},
				},
			},
			expect: []Timeline{
				{Start: 3, End: 6},
				{Start: 14, End: 19},
				{Start: 19, End: 42},
			},
		},
		{
			name: "partly overlap",
			tl: [][]Timeline{
				{
					{Start: 3, End: 8},
					{Start: 14, End: 19},
					{Start: 21, End: 42},
				},
				{
					{Start: 1, End: 3},
					{Start: 4, End: 7},
					{Start: 19, End: 36},
				},
				{
					{Start: 5, End: 8},
					{Start: 14, End: 19},
					{Start: 19, End: 42},
				},
			},
			expect: []Timeline{
				{Start: 5, End: 7},
				{Start: 21, End: 36},
			},
		},
		{
			name: "redundant chunks",
			tl: [][]Timeline{
				{
					{Start: 3, End: 6},
					{Start: 14, End: 19},
					{Start: 19, End: 42},
					{Start: 42, End: 100500},
				},
				{
					{Start: 2, End: 7},
					{Start: 7, End: 8},
					{Start: 8, End: 10},
					{Start: 14, End: 20},
					{Start: 20, End: 30},
				},
				{
					{Start: 1, End: 5},
					{Start: 13, End: 19},
					{Start: 20, End: 30},
				},
			},
			expect: []Timeline{
				{Start: 3, End: 5},
				{Start: 14, End: 19},
				{Start: 20, End: 30},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := MergeTimelines(test.tl...)
			if len(test.expect) != len(got) {
				t.Fatalf("wrong timelines, exepct <%d> %v, got <%d> %v", len(test.expect), printttl(test.expect...), len(got), printttl(got...))
			}
			for i, gl := range got {
				if test.expect[i] != gl {
					t.Errorf("wrong timeline %d, exepct %v, got %v", i, printttl(test.expect[i]), printttl(gl))
				}
			}
		})
	}
}

func printttl(tlns ...Timeline) string {
	ret := []string{}
	for _, t := range tlns {
		ret = append(ret, fmt.Sprintf("[%v - %v]", t.Start, t.End))
	}

	return strings.Join(ret, ", ")
}
