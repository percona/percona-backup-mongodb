package oss

import (
	"context"
	"fmt"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	osscred "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/retry"
	"github.com/aliyun/credentials-go/credentials/providers"
)

const (
	defaultPartSize int64 = 10 * 1024 * 1024 // 10Mb
	defaultS3Region       = "ap-southeast-5"

	defaultRetryBaseDelay    = 30 * time.Millisecond
	defaultRetryerMaxBackoff = 300 * time.Second
)

//nolint:lll
type Config struct {
	Region      string `bson:"region" json:"region" yaml:"region"`
	EndpointURL string `bson:"endpointUrl,omitempty" json:"endpointUrl" yaml:"endpointUrl,omitempty"`

	Bucket      string      `bson:"bucket" json:"bucket" yaml:"bucket"`
	Prefix      string      `bson:"prefix,omitempty" json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Credentials Credentials `bson:"credentials" json:"-" yaml:"credentials"`

	Retryer *Retryer `bson:"retryer,omitempty" json:"retryer,omitempty" yaml:"retryer,omitempty"`

	ConnectTimeout int   `bson:"connectTimeout" json:"connectTimeout" yaml:"connectTimeout"`
	UploadPartSize int   `bson:"uploadPartSize,omitempty" json:"uploadPartSize,omitempty" yaml:"uploadPartSize,omitempty"`
	MaxUploadParts int32 `bson:"maxUploadParts,omitempty" json:"maxUploadParts,omitempty" yaml:"maxUploadParts,omitempty"`
}

type Retryer struct {
	MaxAttempts int           `bson:"maxAttempts" json:"maxAttempts" yaml:"maxAttempts"`
	MaxBackoff  time.Duration `bson:"maxBackoff" json:"maxBackoff" yaml:"maxBackoff"`
	BaseDelay   time.Duration `bson:"baseDelay" json:"baseDelay" yaml:"baseDelay"`
}

type Credentials struct {
	AccessKeyID     string `bson:"accessKeyId" json:"accessKeyId,omitempty" yaml:"accessKeyId,omitempty"`
	AccessKeySecret string `bson:"accessKeySecret" json:"accessKeySecret,omitempty" yaml:"accessKeySecret,omitempty"`
	SecurityToken   string `bson:"securityToken" json:"securityToken,omitempty" yaml:"securityToken,omitempty"`
	RoleARN         string `bson:"roleArn,omitempty" json:"roleArn,omitempty" yaml:"roleArn,omitempty"`
	SessionName     string `bson:"sessionName,omitempty" json:"sessionName,omitempty" yaml:"sessionName,omitempty"`
}

func (cfg *Config) Cast() error {
	if cfg.Region == "" {
		cfg.Region = defaultS3Region
	}
	if cfg.Retryer != nil {
		if cfg.Retryer.BaseDelay == 0 {
			cfg.Retryer.BaseDelay = defaultRetryBaseDelay
		}
		if cfg.Retryer.MaxBackoff == 0 {
			cfg.Retryer.MaxBackoff = defaultRetryerMaxBackoff
		}
	}
	return nil
}

const (
	defaultSessionExpiration = 3600
)

func newCred(config *Config) (*cred, error) {
	var credentialsProvider providers.CredentialsProvider
	var err error

	if config.Credentials.AccessKeyID == "" || config.Credentials.AccessKeySecret == "" {
		return nil, fmt.Errorf("access key ID and secret are required")
	}

	if config.Credentials.SecurityToken != "" {
		credentialsProvider, err = providers.NewStaticSTSCredentialsProviderBuilder().
			WithAccessKeyId(config.Credentials.AccessKeyID).
			WithAccessKeySecret(config.Credentials.AccessKeySecret).
			WithSecurityToken(config.Credentials.SecurityToken).
			Build()
	} else {
		credentialsProvider, err = providers.NewStaticAKCredentialsProviderBuilder().
			WithAccessKeyId(config.Credentials.AccessKeyID).
			WithAccessKeySecret(config.Credentials.AccessKeySecret).
			Build()
	}
	if err != nil {
		return nil, fmt.Errorf("credentials provider: %w", err)
	}

	if config.Credentials.RoleARN != "" {
		internalProvider := credentialsProvider
		credentialsProvider, err = providers.NewRAMRoleARNCredentialsProviderBuilder().
			WithCredentialsProvider(internalProvider).
			WithRoleArn(config.Credentials.RoleARN).
			WithRoleSessionName(config.Credentials.SessionName).
			WithDurationSeconds(defaultSessionExpiration).
			Build()
		if err != nil {
			return nil, fmt.Errorf("ram role credential provider: %w", err)
		}
	}

	return &cred{
		provider: credentialsProvider,
	}, nil
}

type cred struct {
	provider providers.CredentialsProvider
}

func (c *cred) GetCredentials(ctx context.Context) (osscred.Credentials, error) {
	cc, err := c.provider.GetCredentials()
	if err != nil {
		return osscred.Credentials{}, err
	}

	return osscred.Credentials{
		AccessKeyID:     cc.AccessKeyId,
		AccessKeySecret: cc.AccessKeySecret,
		SecurityToken:   cc.SecurityToken,
	}, nil
}

func configureClient(config *Config) (*oss.Client, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if config.Retryer == nil {
		config.Retryer = &Retryer{MaxAttempts: 3, MaxBackoff: defaultRetryerMaxBackoff, BaseDelay: defaultRetryBaseDelay}
	}
	if config.Region == "" || config.Bucket == "" || config.Credentials.AccessKeyID == "" || config.Credentials.AccessKeySecret == "" {
		return nil, fmt.Errorf("Missing required OSS config: %+v", config)
	}

	cred, err := newCred(config)
	if err != nil {
		return nil, fmt.Errorf("create credentials: %w", err)
	}

	ossConfig := oss.LoadDefaultConfig().
		WithRegion(config.Region).
		WithCredentialsProvider(cred).
		WithSignatureVersion(oss.SignatureVersionV4).
		WithRetryMaxAttempts(config.Retryer.MaxAttempts).
		WithRetryer(retry.NewStandard(func(ro *retry.RetryOptions) {
			ro.MaxAttempts = config.Retryer.MaxAttempts
			ro.MaxBackoff = config.Retryer.MaxBackoff
			ro.BaseDelay = config.Retryer.BaseDelay
		})).
		WithConnectTimeout(time.Duration(config.ConnectTimeout) * time.Second)

	if config.EndpointURL != "" {
		ossConfig = ossConfig.WithEndpoint(config.EndpointURL)
	}

	return oss.NewClient(ossConfig), nil
}
