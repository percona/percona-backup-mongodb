package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"io"
	"math"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/chacha20poly1305"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

type EncryptionType string

const (
	EncryptionTypeNone              EncryptionType = "none"
	EncryptionTypeAES256GCM         EncryptionType = "aes-256-gcm"
	EncryptionTypeXChaCha20Poly1305 EncryptionType = "xchacha20-poly1305"
)

const (
	keyLen     = 32 // AES-256 and XChaCha20 both take a 32 byte key
	saltLen    = 16 // key derivation function salt
	counterLen = 4  // per-chunk counter appended to the nonce prefix
	chunkSize  = 64 * 1024

	// Argon2id cost parameters
	// Memory is the dominant defense against offline cracking of a low-entropy
	// passphrase. The target is ~64 MiB and a few hundred ms to derive.
	// Parallelism is fixed so derivation is reproducible.
	argonTime    = 3
	argonMemory  = 64 * 1024 // KiB
	argonThreads = 4

	// The frame header is a big-endian uint32 (frameHeaderLen bytes). The high
	// bit flags the final chunk, the remaining 31 bits hold the ciphertext
	// length.
	frameHeaderLen = 4
	finalBit       = uint32(1) << 31
	maxFrameLen    = finalBit - 1

	formatVersion byte = 1
)

// magic identifies a PBM encrypted stream and guards against feeding the
// decryptor unrelated data.
var magic = [4]byte{'P', 'B', 'M', 'E'}

// algorithm identifiers stored in the stream header. The algorithm is read
// from the authenticated header on decrypt, so the cipher is bound to the
// ciphertext.
const (
	algoAES256GCM         byte = 1
	algoXChaCha20Poly1305 byte = 2
)

func algoID(encryption EncryptionType) (byte, bool) {
	switch encryption {
	case EncryptionTypeAES256GCM:
		return algoAES256GCM, true
	case EncryptionTypeXChaCha20Poly1305:
		return algoXChaCha20Poly1305, true
	default:
		return 0, false
	}
}

func algoType(id byte) (EncryptionType, bool) {
	switch id {
	case algoAES256GCM:
		return EncryptionTypeAES256GCM, true
	case algoXChaCha20Poly1305:
		return EncryptionTypeXChaCha20Poly1305, true
	default:
		return "", false
	}
}

// Byte offsets of the constant-length fields in the stream header.
const (
	versionOffset   = len(magic)
	algorithmOffset = versionOffset + 1
	saltOffset      = algorithmOffset + 1
)

// fixedHeaderLen is the size of the constant-length part of the stream header:
// magic + version + algorithm + salt. It is followed by a nonce prefix whose
// length depends on the cipher's nonce size.
const fixedHeaderLen = saltOffset + saltLen

func IsValidEncryptionType(s string) bool {
	switch EncryptionType(s) {
	case
		EncryptionTypeNone,
		EncryptionTypeAES256GCM,
		EncryptionTypeXChaCha20Poly1305:
		return true
	}

	return false
}

// deriveKey turns a passphrase into a 32 byte key using Argon2id and the
// per-stream salt
func deriveKey(passphrase string, salt []byte) ([]byte, error) {
	if passphrase == "" {
		return nil, errors.New("empty passphrase")
	}
	return argon2.IDKey([]byte(passphrase), salt, argonTime, argonMemory, argonThreads, keyLen), nil
}

// newAEAD builds the AEAD cipher for the given encryption type and key
func newAEAD(encryption EncryptionType, key []byte) (cipher.AEAD, error) {
	switch encryption {
	case EncryptionTypeAES256GCM:
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, errors.Wrap(err, "new aes cipher")
		}
		gcm, err := cipher.NewGCM(block)
		return gcm, errors.Wrap(err, "new aes gcm")
	case EncryptionTypeXChaCha20Poly1305:
		aead, err := chacha20poly1305.NewX(key)
		return aead, errors.Wrap(err, "new xchacha20poly1305")
	default:
		return nil, errors.Errorf("unsupported encryption type %q", encryption)
	}
}

// Encrypt makes a encrypting writer from the given one
func Encrypt(w io.Writer, encryption EncryptionType, passphrase string) (io.WriteCloser, error) {
	switch encryption {
	case EncryptionTypeAES256GCM, EncryptionTypeXChaCha20Poly1305:
		algo, ok := algoID(encryption)
		if !ok {
			return nil, errors.Errorf("unsupported encryption type %q", encryption)
		}

		salt := make([]byte, saltLen)
		if _, err := rand.Read(salt); err != nil {
			return nil, errors.Wrap(err, "generate salt")
		}

		key, err := deriveKey(passphrase, salt)
		if err != nil {
			return nil, err
		}
		aead, err := newAEAD(encryption, key)
		if err != nil {
			return nil, err
		}

		noncePrefix := make([]byte, aead.NonceSize()-counterLen)
		if _, err := rand.Read(noncePrefix); err != nil {
			return nil, errors.Wrap(err, "generate nonce prefix")
		}

		header := make([]byte, 0, fixedHeaderLen+len(noncePrefix))
		header = append(header, magic[:]...)
		header = append(header, formatVersion)
		header = append(header, algo)
		header = append(header, salt...)
		header = append(header, noncePrefix...)

		return &aeadWriter{
			w:           w,
			aead:        aead,
			noncePrefix: noncePrefix,
			header:      header,
			buf:         make([]byte, 0, chunkSize),
		}, nil
	case EncryptionTypeNone:
		fallthrough
	default:
		return nopWriteCloser{w}, nil
	}
}

// Decrypt wraps the given reader by a decrypting io.ReadCloser
func Decrypt(r io.Reader, encryption EncryptionType, passphrase string) (io.ReadCloser, error) {
	switch encryption {
	case EncryptionTypeAES256GCM, EncryptionTypeXChaCha20Poly1305:
		header := make([]byte, fixedHeaderLen)
		if _, err := io.ReadFull(r, header); err != nil {
			return nil, errors.Wrap(err, "read header")
		}
		if [4]byte(header[:versionOffset]) != magic {
			return nil, errors.New("not a PBM encrypted stream")
		}
		if header[versionOffset] != formatVersion {
			return nil, errors.Errorf("unsupported encryption format version %d", header[versionOffset])
		}

		streamEncryption, ok := algoType(header[algorithmOffset])
		if !ok {
			return nil, errors.Errorf("unknown encryption algorithm id %d", header[algorithmOffset])
		}
		// Avoid algorithm downgrade by checking the authenticated header
		if streamEncryption != encryption {
			return nil, errors.Errorf("encryption algorithm mismatch: stream is %q, expected %q",
				streamEncryption, encryption)
		}
		salt := header[saltOffset : saltOffset+saltLen]

		key, err := deriveKey(passphrase, salt)
		if err != nil {
			return nil, err
		}
		aead, err := newAEAD(streamEncryption, key)
		if err != nil {
			return nil, err
		}

		noncePrefix := make([]byte, aead.NonceSize()-counterLen)
		if _, err := io.ReadFull(r, noncePrefix); err != nil {
			return nil, errors.Wrap(err, "read nonce prefix")
		}
		header = append(header, noncePrefix...)

		return &aeadReader{r: r, aead: aead, noncePrefix: noncePrefix, header: header}, nil
	case EncryptionTypeNone:
		fallthrough
	default:
		return io.NopCloser(r), nil
	}
}

// aeadWriter buffers plaintext into fixed-size chunks, sealing each with a
// distinct counter-based nonce. Every chunk is authenticated with the stream
// header (magic, version, algorithm, salt, nonce prefix) and a one-byte
// "final" flag as additional data, so a tampered header or a truncated stream
// cannot be passed off as valid.
type aeadWriter struct {
	w             io.Writer
	aead          cipher.AEAD
	noncePrefix   []byte
	header        []byte
	headerWritten bool
	counter       uint32
	exhausted     bool
	buf           []byte
	closed        bool
}

func (e *aeadWriter) Write(p []byte) (int, error) {
	if e.closed {
		return 0, errors.New("write to closed encrypt writer")
	}
	e.buf = append(e.buf, p...)
	for len(e.buf) >= chunkSize {
		if err := e.seal(e.buf[:chunkSize], false); err != nil {
			return 0, err
		}
		e.buf = e.buf[chunkSize:]
	}
	return len(p), nil
}

func (e *aeadWriter) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	return e.seal(e.buf, true)
}

func (e *aeadWriter) seal(plaintext []byte, final bool) error {
	if e.exhausted {
		return errors.New("chunk counter overflow: stream too large to encrypt safely")
	}

	if !e.headerWritten {
		if _, err := e.w.Write(e.header); err != nil {
			return errors.Wrap(err, "write header")
		}
		e.headerWritten = true
	}

	counter := e.counter
	nonce := e.nextNonce()
	ciphertext := e.aead.Seal(nil, nonce, plaintext, chunkAAD(e.header, counter, final))
	if uint32(len(ciphertext)) > maxFrameLen {
		return errors.New("encrypted chunk too large")
	}

	frameHeader := uint32(len(ciphertext))
	if final {
		frameHeader |= finalBit
	}

	var hdr [frameHeaderLen]byte
	binary.BigEndian.PutUint32(hdr[:], frameHeader)
	if _, err := e.w.Write(hdr[:]); err != nil {
		return errors.Wrap(err, "write frame header")
	}
	if _, err := e.w.Write(ciphertext); err != nil {
		return errors.Wrap(err, "write frame")
	}
	return nil
}

func (e *aeadWriter) nextNonce() []byte {
	nonce := make([]byte, e.aead.NonceSize())
	copy(nonce, e.noncePrefix)
	binary.BigEndian.PutUint32(nonce[len(e.noncePrefix):], e.counter)
	if e.counter == math.MaxUint32 {
		e.exhausted = true
	} else {
		e.counter++
	}
	return nonce
}

// aeadReader reads framed chunks, verifies and decrypts each, and surfaces the
// plaintext as a stream
type aeadReader struct {
	r           io.Reader
	aead        cipher.AEAD
	noncePrefix []byte
	header      []byte
	counter     uint32
	exhausted   bool
	buf         []byte
	eof         bool
}

func (d *aeadReader) Read(p []byte) (int, error) {
	for len(d.buf) == 0 {
		if d.eof {
			return 0, io.EOF
		}
		if err := d.readFrame(); err != nil {
			return 0, err
		}
	}
	n := copy(p, d.buf)
	d.buf = d.buf[n:]
	return n, nil
}

func (d *aeadReader) Close() error { return nil }

func (d *aeadReader) readFrame() error {
	var hdr [frameHeaderLen]byte
	if _, err := io.ReadFull(d.r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return errors.New("truncated encrypted stream")
		}
		return errors.Wrap(err, "read frame header")
	}
	frameHeader := binary.BigEndian.Uint32(hdr[:])
	final := frameHeader&finalBit != 0
	length := frameHeader &^ finalBit

	// The frame header is not authenticated, so a tampered stream can claim any
	// length. Reject anything larger than a legitimate sealed chunk before
	// allocating, otherwise a forged length triggers an unbounded allocation.
	if length > uint32(chunkSize+d.aead.Overhead()) {
		return errors.Errorf("encrypted frame length exceeds maximum")
	}

	ciphertext := make([]byte, length)
	if _, err := io.ReadFull(d.r, ciphertext); err != nil {
		return errors.Wrap(err, "read frame")
	}

	if d.exhausted {
		return errors.New("chunk counter overflow: stream has too many chunks")
	}
	counter := d.counter
	nonce := d.nextNonce()
	plaintext, err := d.aead.Open(nil, nonce, ciphertext, chunkAAD(d.header, counter, final))
	if err != nil {
		return errors.Wrap(err, "decrypt frame")
	}

	d.buf = plaintext
	if final {
		d.eof = true
		// The final chunk must be the end of the stream. Anything trailing it
		// is unauthenticated and indicates tampering or a spliced stream.
		var probe [1]byte
		if _, err := io.ReadFull(d.r, probe[:]); !errors.Is(err, io.EOF) {
			if err == nil {
				return errors.New("trailing data after final chunk")
			}
			return errors.Wrap(err, "check for trailing data")
		}
	}
	return nil
}

func (d *aeadReader) nextNonce() []byte {
	nonce := make([]byte, d.aead.NonceSize())
	copy(nonce, d.noncePrefix)
	binary.BigEndian.PutUint32(nonce[len(d.noncePrefix):], d.counter)
	if d.counter == math.MaxUint32 {
		d.exhausted = true
	} else {
		d.counter++
	}
	return nonce
}

// chunkAAD returns the additional authenticated data for a chunk:
//   - The full stream header so the algorithm, version, salt, and nonce prefix
//     cannot be tampered with
//   - The chunk counter so chunks cannot be reordered or spliced in from
//     another point in the stream
//   - A one-byte final flag so dropping the last chunk is detectable
func chunkAAD(header []byte, counter uint32, final bool) []byte {
	aad := make([]byte, len(header)+counterLen+1)
	copy(aad, header)
	binary.BigEndian.PutUint32(aad[len(header):], counter)
	if final {
		aad[len(header)+counterLen] = 1
	}
	return aad
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }
