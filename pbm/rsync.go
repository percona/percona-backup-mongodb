package pbm

import (
	"encoding/json"
	"log"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

func (p *PBM) ResyncBackupList() error {
	stg, err := p.GetStorage()
	if err != nil {
		return errors.Wrap(err, "unable to get backup store")
	}
	var bcps []BackupMeta
	switch stg.Type {
	default:
		return errors.New("store is doesn't set, you have to set store to make backup")
	case StorageS3:
		bcps, err = getBackupListS3(stg)
		if err != nil {
			return errors.Wrap(err, "")
		}

		// COPY ALL FROM pbmBcp
		// INSETR bcps
	}

	return nil
}

func getBackupListS3(stg Storage) ([]BackupMeta, error) {
	awsSession, err := session.NewSession(&aws.Config{
		Region:   aws.String(stg.S3.Region),
		Endpoint: aws.String(stg.S3.EndpointURL),
		Credentials: credentials.NewStaticCredentials(
			stg.S3.Credentials.AccessKeyID,
			stg.S3.Credentials.SecretAccessKey,
			"",
		),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, errors.Wrap(err, "create AWS session")
	}

	lparams := &s3.ListObjectsV2Input{
		Bucket:    aws.String(stg.S3.Bucket),
		Delimiter: aws.String("/"),
	}
	if stg.S3.Prefix != "" {
		lparams.Prefix = aws.String(stg.S3.Prefix)
	}

	bcps := []BackupMeta{}
	awscli := s3.New(awsSession)
	var berr error
	err = awscli.ListObjectsV2Pages(lparams,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, o := range page.Contents {
				name := aws.StringValue(o.Key)
				if strings.HasSuffix(name, ".pbm.json") {
					log.Println(name)
					s3obj, err := awscli.GetObject(&s3.GetObjectInput{
						Bucket: aws.String(stg.S3.Bucket),
						Key:    aws.String(path.Join(stg.S3.Prefix, name)),
					})
					if err != nil {
						berr = errors.Wrapf(err, "get object '%s'", name)
						return false
					}

					m := BackupMeta{}
					berr = json.NewDecoder(s3obj.Body).Decode(&m)
					if err != nil {
						berr = errors.Wrapf(err, "decode object '%s'", name)
						return false
					}
					if m.Name != "" {
						bcps = append(bcps, m)
					}
				}
			}
			return true
		})

	if err != nil {
		return nil, errors.Wrap(err, "get backup list")
	}

	if berr != nil {
		return nil, errors.Wrap(berr, "metadata")
	}

	return bcps, nil
}
