module github.com/percona/percona-backup-mongodb

go 1.19

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.3.0
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.0.0
	github.com/alecthomas/kingpin v2.2.6+incompatible
	github.com/aws/aws-sdk-go v1.44.206
	github.com/docker/docker v20.10.21+incompatible
	github.com/golang/snappy v0.0.4
	github.com/google/uuid v1.3.0
	github.com/klauspost/compress v1.15.15
	github.com/klauspost/pgzip v1.2.5
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/mongodb/mongo-tools v0.0.0-20221102190735-6d9d341edd33
	github.com/pierrec/lz4 v2.6.1+incompatible
	github.com/pkg/errors v0.9.1
	go.mongodb.org/mongo-driver v1.11.2
	golang.org/x/mod v0.8.0
	golang.org/x/sync v0.1.0
	gopkg.in/yaml.v2 v2.4.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.1.1 // indirect
	github.com/Microsoft/go-winio v0.6.0 // indirect
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751 // indirect
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137 // indirect
	github.com/docker/distribution v2.8.2+incompatible // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/frankban/quicktest v1.14.4 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/jessevdk/go-flags v1.5.0 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/montanaflynn/stats v0.6.6 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20201027041543-1326539a0a0a // indirect
	golang.org/x/crypto v0.4.0 // indirect
	golang.org/x/net v0.7.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/term v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	golang.org/x/tools v0.4.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
)

replace (
	github.com/docker/docker => github.com/docker/docker v1.13.1
	github.com/mongodb/mongo-tools => github.com/mongodb/mongo-tools v0.0.0-20221102190735-6d9d341edd33
)
