package speedt

import (
	"io/ioutil"
	"testing"
)

func BenchmarkWriterBytes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ioutil.Discard.Write([]byte(dataset[i%len(dataset)]))
	}
}

func BenchmarkWriterUnsafe(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ioutil.Discard.Write(StringToBytes(dataset[i%len(dataset)]))
	}
}
