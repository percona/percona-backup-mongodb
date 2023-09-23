package main

import (
	"io"
	"testing"
)

func BenchmarkWriterBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = io.Discard.Write([]byte(dataset[i%len(dataset)]))
	}
}

func BenchmarkWriterUnsafe(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = io.Discard.Write(StringToBytes(dataset[i%len(dataset)]))
	}
}
