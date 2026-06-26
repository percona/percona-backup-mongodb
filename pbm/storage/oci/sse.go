package oci

import (
	"crypto/sha256"
	"encoding/base64"

	"github.com/oracle/oci-go-sdk/v65/common"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

const sseCustomerAlgorithm = "AES256"

type sseHeaders struct {
	kmsKeyID          *string
	customerAlgorithm *string
	customerKey       *string
	customerKeySHA256 *string
}

func sseHeadersFor(sse SSE) (sseHeaders, error) {
	if sse.KmsKeyID != "" && sse.SseCustomerKey != "" {
		return sseHeaders{}, errors.New("kmsKeyID cannot be used with SSE-C")
	}
	if sse.KmsKeyID != "" {
		return sseHeaders{kmsKeyID: common.String(sse.KmsKeyID)}, nil
	}
	if sse.SseCustomerKey == "" {
		return sseHeaders{}, nil
	}

	keySHA256, err := sseCustomerKeySHA256(string(sse.SseCustomerKey))
	if err != nil {
		return sseHeaders{}, err
	}

	return sseHeaders{
		customerAlgorithm: common.String(sseCustomerAlgorithm),
		customerKey:       common.String(string(sse.SseCustomerKey)),
		customerKeySHA256: common.String(keySHA256),
	}, nil
}

func validateSSE(sse SSE) error {
	hasKMS := sse.KmsKeyID != ""
	hasSSECKey := sse.SseCustomerKey != ""

	switch {
	case !hasKMS && !hasSSECKey:
		return nil
	case hasKMS && hasSSECKey:
		return errors.New("kmsKeyID cannot be used with SSE-C")
	case hasKMS:
		return nil
	}

	_, err := sseCustomerKeySHA256(string(sse.SseCustomerKey))
	return err
}

func sseCustomerKeySHA256(key string) (string, error) {
	decodedKey, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		return "", errors.Wrap(err, "decode sseCustomerKey")
	}
	sum := sha256.Sum256(decodedKey)

	return base64.StdEncoding.EncodeToString(sum[:]), nil
}
