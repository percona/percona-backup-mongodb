package encrypt

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"
)

const passphrase = "correct horse battery staple"

func allTypes() []EncryptionType {
	return []EncryptionType{
		EncryptionTypeNone,
		EncryptionTypeAES256GCM,
		EncryptionTypeXChaCha20Poly1305,
	}
}

func encryptingTypes() []EncryptionType {
	return []EncryptionType{
		EncryptionTypeAES256GCM,
		EncryptionTypeXChaCha20Poly1305,
	}
}

// noncePrefixLen is the length of the random nonce prefix stored in the header
// for the cipher. It depends on the AEAD nonce size.
func noncePrefixLen(t *testing.T, enc EncryptionType) int {
	t.Helper()
	aead, err := newAEAD(enc, make([]byte, keyLen))
	if err != nil {
		t.Fatalf("%s: newAEAD: %v", enc, err)
	}
	return aead.NonceSize() - counterLen
}

// roundTrip encrypts then decrypts data
func roundTrip(t *testing.T, enc EncryptionType, plaintext []byte) {
	t.Helper()

	var buf bytes.Buffer
	w, err := Encrypt(&buf, enc, passphrase)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if _, err := w.Write(plaintext); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if enc != EncryptionTypeNone && len(plaintext) >= 64 && bytes.Contains(buf.Bytes(), plaintext) {
		t.Fatalf("%s: ciphertext contains plaintext", enc)
	}

	r, err := Decrypt(&buf, enc, passphrase)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, plaintext) {
		t.Fatalf("%s: round trip mismatch: got %d bytes, want %d", enc, len(got), len(plaintext))
	}
}

func TestRoundTrip(t *testing.T) {
	sizes := []int{0, 1, chunkSize - 1, chunkSize, chunkSize + 1, 5 * chunkSize, 5*chunkSize + 123}
	for _, enc := range allTypes() {
		t.Run(string(enc), func(t *testing.T) {
			for _, size := range sizes {
				data := make([]byte, size)
				if _, err := rand.Read(data); err != nil {
					t.Fatal(err)
				}
				roundTrip(t, enc, data)
			}
		})
	}
}

// TestHeaderWrittenLazily guards a deadlock. Encrypt must not write
// the stream header to the writer until the first frame. Storage upload builds
// the encrypting writer on the write end of an unbuffered io.Pipe and only
// then starts the reader, so an eager header write blocks forever.
func TestHeaderWrittenLazily(t *testing.T) {
	for _, enc := range encryptingTypes() {
		t.Run(string(enc), func(t *testing.T) {
			var buf bytes.Buffer
			w, err := Encrypt(&buf, enc, passphrase)
			if err != nil {
				t.Fatalf("Encrypt: %v", err)
			}
			if buf.Len() != 0 {
				t.Fatalf("Encrypt wrote %d bytes before any frame, want 0", buf.Len())
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if buf.Len() == 0 {
				t.Fatal("nothing written after Close")
			}
		})
	}
}

// TestEncryptThroughPipe reproduces the storage.Upload wiring. The encrypting
// writer is created on a pipe before its reader goroutine starts. With an eager
// header write this deadlocks.
func TestEncryptThroughPipe(t *testing.T) {
	for _, enc := range encryptingTypes() {
		t.Run(string(enc), func(t *testing.T) {
			plaintext := make([]byte, 3*chunkSize+7)
			if _, err := rand.Read(plaintext); err != nil {
				t.Fatal(err)
			}

			r, pw := io.Pipe()
			w, err := Encrypt(pw, enc, passphrase)
			if err != nil {
				t.Fatalf("Encrypt: %v", err)
			}

			go func() {
				_, werr := w.Write(plaintext)
				cerr := w.Close()
				pw.CloseWithError(errors.Join(werr, cerr))
			}()

			dr, err := Decrypt(r, enc, passphrase)
			if err != nil {
				t.Fatalf("Decrypt: %v", err)
			}
			got, err := io.ReadAll(dr)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if !bytes.Equal(got, plaintext) {
				t.Fatalf("round trip mismatch: got %d bytes, want %d", len(got), len(plaintext))
			}
		})
	}
}

func TestWrongPassphraseFails(t *testing.T) {
	for _, enc := range encryptingTypes() {
		t.Run(string(enc), func(t *testing.T) {
			var buf bytes.Buffer
			w, err := Encrypt(&buf, enc, passphrase)
			if err != nil {
				t.Fatal(err)
			}
			w.Write([]byte("top secret backup payload"))
			w.Close()

			r, err := Decrypt(&buf, enc, "wrong passphrase")
			if err != nil {
				t.Fatalf("Decrypt setup: %v", err)
			}
			if _, err := io.ReadAll(r); err == nil {
				t.Fatal("expected authentication failure with wrong passphrase")
			}
		})
	}
}

func TestTruncationDetected(t *testing.T) {
	for _, enc := range encryptingTypes() {
		t.Run(string(enc), func(t *testing.T) {
			var buf bytes.Buffer
			w, _ := Encrypt(&buf, enc, passphrase)
			data := make([]byte, 3*chunkSize)
			rand.Read(data)
			w.Write(data)
			w.Close()

			// Drop the final frame entirely by keeping only the header and first frame.
			full := buf.Bytes()
			// header + nonce prefix + partial
			truncated := full[:fixedHeaderLen+noncePrefixLen(t, enc)+frameHeaderLen+chunkSize/2]

			r, err := Decrypt(bytes.NewReader(truncated), enc, passphrase)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadAll(r); err == nil {
				t.Fatal("expected error on truncated stream")
			}
		})
	}
}

func TestEmptyPassphraseRejected(t *testing.T) {
	for _, enc := range encryptingTypes() {
		t.Run(string(enc), func(t *testing.T) {
			if _, err := Encrypt(io.Discard, enc, ""); err == nil {
				t.Fatal("expected error for empty passphrase")
			}
		})
	}
}

func TestAlgorithmMismatchRejected(t *testing.T) {
	pairs := []struct{ stream, hint EncryptionType }{
		{EncryptionTypeAES256GCM, EncryptionTypeXChaCha20Poly1305},
		{EncryptionTypeXChaCha20Poly1305, EncryptionTypeAES256GCM},
	}
	for _, p := range pairs {
		t.Run(string(p.stream)+"_as_"+string(p.hint), func(t *testing.T) {
			var buf bytes.Buffer
			w, err := Encrypt(&buf, p.stream, passphrase)
			if err != nil {
				t.Fatal(err)
			}
			w.Write([]byte("top secret backup payload"))
			w.Close()

			if _, err := Decrypt(&buf, p.hint, passphrase); err == nil {
				t.Fatal("expected mismatch error when decrypt hint disagrees with stream algorithm")
			}
		})
	}
}

func TestCounterOverflowRejected(t *testing.T) {
	for _, enc := range encryptingTypes() {
		t.Run(string(enc), func(t *testing.T) {
			var buf bytes.Buffer
			wc, err := Encrypt(&buf, enc, passphrase)
			if err != nil {
				t.Fatal(err)
			}
			w := wc.(*aeadWriter)
			w.counter = math.MaxUint32

			if _, err := w.Write(make([]byte, chunkSize)); err != nil {
				t.Fatalf("sealing the last valid chunk should succeed: %v", err)
			}
			if _, err := w.Write(make([]byte, chunkSize)); err == nil {
				t.Fatal("expected counter overflow error on the chunk past the limit")
			}
		})
	}
}

func TestTrailingDataDetected(t *testing.T) {
	for _, enc := range encryptingTypes() {
		t.Run(string(enc), func(t *testing.T) {
			var buf bytes.Buffer
			w, _ := Encrypt(&buf, enc, passphrase)
			w.Write([]byte("top secret backup payload"))
			w.Close()

			extended := append(buf.Bytes(), 0x00)

			r, err := Decrypt(bytes.NewReader(extended), enc, passphrase)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadAll(r); err == nil {
				t.Fatal("expected failure for data appended after the final chunk")
			}
		})
	}
}

func TestChunkReorderDetected(t *testing.T) {
	for _, enc := range encryptingTypes() {
		t.Run(string(enc), func(t *testing.T) {
			var buf bytes.Buffer
			w, _ := Encrypt(&buf, enc, passphrase)
			data := make([]byte, 2*chunkSize+100)
			rand.Read(data)
			w.Write(data)
			w.Close()

			full := buf.Bytes()
			hdrLen := fixedHeaderLen + noncePrefixLen(t, enc)
			body := full[hdrLen:]

			frame := func(off int) []byte {
				length := int(binary.BigEndian.Uint32(body[off:off+frameHeaderLen]) &^ finalBit)
				return body[off : off+frameHeaderLen+length]
			}
			f0 := frame(0)
			f1 := frame(len(f0))

			reordered := append([]byte(nil), full[:hdrLen]...)
			reordered = append(reordered, f1...)
			reordered = append(reordered, f0...)
			reordered = append(reordered, body[len(f0)+len(f1):]...)

			r, err := Decrypt(bytes.NewReader(reordered), enc, passphrase)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadAll(r); err == nil {
				t.Fatal("expected failure when chunks are reordered")
			}
		})
	}
}

// magic, version, algorithm, salt, and nonce prefix are all authenticated,
// so flipping any header byte must be detected rather than silently
// changing how the stream is decrypted
func TestHeaderTamperDetected(t *testing.T) {
	for _, enc := range encryptingTypes() {
		t.Run(string(enc), func(t *testing.T) {
			noncePrefixInterior := fixedHeaderLen + noncePrefixLen(t, enc)/2
			for _, off := range []int{algorithmOffset, saltOffset, fixedHeaderLen, noncePrefixInterior} {
				var buf bytes.Buffer
				w, _ := Encrypt(&buf, enc, passphrase)
				data := make([]byte, chunkSize)
				rand.Read(data)
				w.Write(data)
				w.Close()

				tampered := append([]byte(nil), buf.Bytes()...)
				tampered[off] ^= 0xff

				r, err := Decrypt(bytes.NewReader(tampered), enc, passphrase)
				if err != nil {
					continue // rejected at setup (e.g. unknown algorithm id)
				}
				if _, err := io.ReadAll(r); err == nil {
					t.Fatalf("expected failure for header tampered at offset %d", off)
				}
			}
		})
	}
}
