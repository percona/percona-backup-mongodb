package s3

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/corehandlers"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2"
)

// GcpTokenProvider implements credentials.Provider interface
type GcpTokenProvider struct {
	token *oauth2.Token
}

func (p *GcpTokenProvider) Retrieve() (credentials.Value, error) {
	ctx := context.Background()

	creds, err := gcp.DefaultCredentials(ctx)
	if err != nil {
		return credentials.Value{}, err
	}

	tokenSource := gcp.CredentialsTokenSource(creds)

	token, err := tokenSource.Token()
	if err != nil {
		return credentials.Value{}, err
	}

	p.token = token

	return credentials.Value{
		AccessKeyID:     token.AccessToken,
		SecretAccessKey: creds.ProjectID,
		SessionToken:    "",
		ProviderName:    "",
	}, nil
}

func (p *GcpTokenProvider) IsExpired() bool {
	if p.token == nil {
		return true
	}

	// token has no expiration
	if p.token.Expiry.IsZero() {
		return false
	}

	if p.token.Expiry.Before(time.Now()) {
		return true
	}

	return false
}

func adaptS3Client(client *s3.S3) {
	// modify HTTP request
	client.Handlers.Sign.Clear()
	client.Handlers.Sign.PushBackNamed(corehandlers.BuildContentLengthHandler)
	client.Handlers.Sign.PushBackNamed(request.NamedHandler{Name: "gcp-oauth2-signer", Fn: func(req *request.Request) {
		credValue, err := client.Config.Credentials.Get()
		if err != nil {
			return
		}

		// add authorization header
		req.HTTPRequest.Header.Set("Authorization", fmt.Sprintf("Bearer %s", credValue.AccessKeyID))
		req.HTTPRequest.Header.Set("x-goog-project-id", credValue.SecretAccessKey)
	}})

	client.Handlers.Send.PushFrontNamed(request.NamedHandler{Name: "aws-to-gcp-headers", Fn: func(req *request.Request) {
		// add "x-goog-*" headers corresponding to "x-amz-*" headers
		for key := range req.HTTPRequest.Header {
			lowerKey := strings.ToLower(key)
			if strings.HasPrefix(lowerKey, "x-amz-") {
				newKey := strings.Replace(lowerKey, "x-amz-", "x-goog-", 1)
				req.HTTPRequest.Header.Set(newKey, req.HTTPRequest.Header.Get(key))
			}
		}

		// remove "x-amz-*" headers
		for key := range req.HTTPRequest.Header {
			if strings.HasPrefix(strings.ToLower(key), "x-amz-") {
				req.HTTPRequest.Header.Del(key)
			}
		}
	}})

	// modify HTTP response
	client.Handlers.Send.PushBackNamed(request.NamedHandler{Name: "gcp-to-aws-headers", Fn: func(req *request.Request) {
		// add "x-amz-*" headers corresponding to "x-goog-*" headers
		for key := range req.HTTPResponse.Header {
			lowerKey := strings.ToLower(key)
			if strings.HasPrefix(lowerKey, "x-goog-") {
				newKey := strings.Replace(lowerKey, "x-goog-", "x-amz-", 1)
				req.HTTPResponse.Header.Set(newKey, req.HTTPResponse.Header.Get(key))
			}
		}

		// remove "x-goog-*" headers
		for key := range req.HTTPResponse.Header {
			if strings.HasPrefix(strings.ToLower(key), "x-goog-") {
				req.HTTPResponse.Header.Del(key)
			}
		}
	}})
}
