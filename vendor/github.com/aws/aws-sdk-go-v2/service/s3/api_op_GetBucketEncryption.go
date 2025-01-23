// Code generated by smithy-go-codegen DO NOT EDIT.

package s3

import (
	"context"
	"fmt"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	s3cust "github.com/aws/aws-sdk-go-v2/service/s3/internal/customizations"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/aws/smithy-go/ptr"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// Returns the default encryption configuration for an Amazon S3 bucket. By
// default, all buckets have a default encryption configuration that uses
// server-side encryption with Amazon S3 managed keys (SSE-S3).
//
//   - General purpose buckets - For information about the bucket default
//     encryption feature, see [Amazon S3 Bucket Default Encryption]in the Amazon S3 User Guide.
//
//   - Directory buckets - For directory buckets, there are only two supported
//     options for server-side encryption: SSE-S3 and SSE-KMS. For information about
//     the default encryption configuration in directory buckets, see [Setting default server-side encryption behavior for directory buckets].
//
// Permissions
//
//   - General purpose bucket permissions - The s3:GetEncryptionConfiguration
//     permission is required in a policy. The bucket owner has this permission by
//     default. The bucket owner can grant this permission to others. For more
//     information about permissions, see [Permissions Related to Bucket Operations]and [Managing Access Permissions to Your Amazon S3 Resources].
//
//   - Directory bucket permissions - To grant access to this API operation, you
//     must have the s3express:GetEncryptionConfiguration permission in an IAM
//     identity-based policy instead of a bucket policy. Cross-account access to this
//     API operation isn't supported. This operation can only be performed by the
//     Amazon Web Services account that owns the resource. For more information about
//     directory bucket policies and permissions, see [Amazon Web Services Identity and Access Management (IAM) for S3 Express One Zone]in the Amazon S3 User Guide.
//
// HTTP Host header syntax  Directory buckets - The HTTP Host header syntax is
// s3express-control.region-code.amazonaws.com .
//
// The following operations are related to GetBucketEncryption :
//
// [PutBucketEncryption]
//
// [DeleteBucketEncryption]
//
// [DeleteBucketEncryption]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketEncryption.html
// [PutBucketEncryption]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketEncryption.html
// [Setting default server-side encryption behavior for directory buckets]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-bucket-encryption.html
// [Amazon S3 Bucket Default Encryption]: https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-encryption.html
// [Managing Access Permissions to Your Amazon S3 Resources]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-access-control.html
// [Permissions Related to Bucket Operations]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-actions.html#using-with-s3-actions-related-to-bucket-subresources
// [Amazon Web Services Identity and Access Management (IAM) for S3 Express One Zone]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-security-iam.html
func (c *Client) GetBucketEncryption(ctx context.Context, params *GetBucketEncryptionInput, optFns ...func(*Options)) (*GetBucketEncryptionOutput, error) {
	if params == nil {
		params = &GetBucketEncryptionInput{}
	}

	result, metadata, err := c.invokeOperation(ctx, "GetBucketEncryption", params, optFns, c.addOperationGetBucketEncryptionMiddlewares)
	if err != nil {
		return nil, err
	}

	out := result.(*GetBucketEncryptionOutput)
	out.ResultMetadata = metadata
	return out, nil
}

type GetBucketEncryptionInput struct {

	// The name of the bucket from which the server-side encryption configuration is
	// retrieved.
	//
	// Directory buckets - When you use this operation with a directory bucket, you
	// must use path-style requests in the format
	// https://s3express-control.region-code.amazonaws.com/bucket-name .
	// Virtual-hosted-style requests aren't supported. Directory bucket names must be
	// unique in the chosen Zone (Availability Zone or Local Zone). Bucket names must
	// also follow the format bucket-base-name--zone-id--x-s3 (for example,
	// DOC-EXAMPLE-BUCKET--usw2-az1--x-s3 ). For information about bucket naming
	// restrictions, see [Directory bucket naming rules]in the Amazon S3 User Guide
	//
	// [Directory bucket naming rules]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-bucket-naming-rules.html
	//
	// This member is required.
	Bucket *string

	// The account ID of the expected bucket owner. If the account ID that you provide
	// does not match the actual owner of the bucket, the request fails with the HTTP
	// status code 403 Forbidden (access denied).
	//
	// For directory buckets, this header is not supported in this API operation. If
	// you specify this header, the request fails with the HTTP status code 501 Not
	// Implemented .
	ExpectedBucketOwner *string

	noSmithyDocumentSerde
}

func (in *GetBucketEncryptionInput) bindEndpointParams(p *EndpointParameters) {

	p.Bucket = in.Bucket
	p.UseS3ExpressControlEndpoint = ptr.Bool(true)
}

type GetBucketEncryptionOutput struct {

	// Specifies the default server-side-encryption configuration.
	ServerSideEncryptionConfiguration *types.ServerSideEncryptionConfiguration

	// Metadata pertaining to the operation's result.
	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}

func (c *Client) addOperationGetBucketEncryptionMiddlewares(stack *middleware.Stack, options Options) (err error) {
	if err := stack.Serialize.Add(&setOperationInputMiddleware{}, middleware.After); err != nil {
		return err
	}
	err = stack.Serialize.Add(&awsRestxml_serializeOpGetBucketEncryption{}, middleware.After)
	if err != nil {
		return err
	}
	err = stack.Deserialize.Add(&awsRestxml_deserializeOpGetBucketEncryption{}, middleware.After)
	if err != nil {
		return err
	}
	if err := addProtocolFinalizerMiddlewares(stack, options, "GetBucketEncryption"); err != nil {
		return fmt.Errorf("add protocol finalizers: %v", err)
	}

	if err = addlegacyEndpointContextSetter(stack, options); err != nil {
		return err
	}
	if err = addSetLoggerMiddleware(stack, options); err != nil {
		return err
	}
	if err = addClientRequestID(stack); err != nil {
		return err
	}
	if err = addComputeContentLength(stack); err != nil {
		return err
	}
	if err = addResolveEndpointMiddleware(stack, options); err != nil {
		return err
	}
	if err = addComputePayloadSHA256(stack); err != nil {
		return err
	}
	if err = addRetry(stack, options); err != nil {
		return err
	}
	if err = addRawResponseToMetadata(stack); err != nil {
		return err
	}
	if err = addRecordResponseTiming(stack); err != nil {
		return err
	}
	if err = addSpanRetryLoop(stack, options); err != nil {
		return err
	}
	if err = addClientUserAgent(stack, options); err != nil {
		return err
	}
	if err = smithyhttp.AddErrorCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = smithyhttp.AddCloseResponseBodyMiddleware(stack); err != nil {
		return err
	}
	if err = addSetLegacyContextSigningOptionsMiddleware(stack); err != nil {
		return err
	}
	if err = addPutBucketContextMiddleware(stack); err != nil {
		return err
	}
	if err = addTimeOffsetBuild(stack, c); err != nil {
		return err
	}
	if err = addUserAgentRetryMode(stack, options); err != nil {
		return err
	}
	if err = addIsExpressUserAgent(stack); err != nil {
		return err
	}
	if err = addOpGetBucketEncryptionValidationMiddleware(stack); err != nil {
		return err
	}
	if err = stack.Initialize.Add(newServiceMetadataMiddleware_opGetBucketEncryption(options.Region), middleware.Before); err != nil {
		return err
	}
	if err = addMetadataRetrieverMiddleware(stack); err != nil {
		return err
	}
	if err = addRecursionDetection(stack); err != nil {
		return err
	}
	if err = addGetBucketEncryptionUpdateEndpoint(stack, options); err != nil {
		return err
	}
	if err = addResponseErrorMiddleware(stack); err != nil {
		return err
	}
	if err = v4.AddContentSHA256HeaderMiddleware(stack); err != nil {
		return err
	}
	if err = disableAcceptEncodingGzip(stack); err != nil {
		return err
	}
	if err = addRequestResponseLogging(stack, options); err != nil {
		return err
	}
	if err = addDisableHTTPSMiddleware(stack, options); err != nil {
		return err
	}
	if err = addSerializeImmutableHostnameBucketMiddleware(stack, options); err != nil {
		return err
	}
	if err = addSpanInitializeStart(stack); err != nil {
		return err
	}
	if err = addSpanInitializeEnd(stack); err != nil {
		return err
	}
	if err = addSpanBuildRequestStart(stack); err != nil {
		return err
	}
	if err = addSpanBuildRequestEnd(stack); err != nil {
		return err
	}
	return nil
}

func (v *GetBucketEncryptionInput) bucket() (string, bool) {
	if v.Bucket == nil {
		return "", false
	}
	return *v.Bucket, true
}

func newServiceMetadataMiddleware_opGetBucketEncryption(region string) *awsmiddleware.RegisterServiceMetadata {
	return &awsmiddleware.RegisterServiceMetadata{
		Region:        region,
		ServiceID:     ServiceID,
		OperationName: "GetBucketEncryption",
	}
}

// getGetBucketEncryptionBucketMember returns a pointer to string denoting a
// provided bucket member valueand a boolean indicating if the input has a modeled
// bucket name,
func getGetBucketEncryptionBucketMember(input interface{}) (*string, bool) {
	in := input.(*GetBucketEncryptionInput)
	if in.Bucket == nil {
		return nil, false
	}
	return in.Bucket, true
}
func addGetBucketEncryptionUpdateEndpoint(stack *middleware.Stack, options Options) error {
	return s3cust.UpdateEndpoint(stack, s3cust.UpdateEndpointOptions{
		Accessor: s3cust.UpdateEndpointParameterAccessor{
			GetBucketFromInput: getGetBucketEncryptionBucketMember,
		},
		UsePathStyle:                   options.UsePathStyle,
		UseAccelerate:                  options.UseAccelerate,
		SupportsAccelerate:             true,
		TargetS3ObjectLambda:           false,
		EndpointResolver:               options.EndpointResolver,
		EndpointResolverOptions:        options.EndpointOptions,
		UseARNRegion:                   options.UseARNRegion,
		DisableMultiRegionAccessPoints: options.DisableMultiRegionAccessPoints,
	})
}
