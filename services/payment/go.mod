module github.com/palemoky/oppenheimer/services/payment

go 1.27

require (
	github.com/palemoky/oppenheimer/api v0.0.0
	github.com/palemoky/oppenheimer/pkg v0.0.0
	github.com/redis/go-redis/v9 v9.23.0
	go.yaml.in/yaml/v3 v3.0.6
	google.golang.org/grpc v1.85.0
	google.golang.org/protobuf v1.36.12
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-sql-driver/mysql v1.10.1 // indirect
	github.com/klauspost/compress v1.20.4 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/pressly/goose/v3 v3.30.0 // indirect
	github.com/sethvargo/go-retry v0.5.0 // indirect
	github.com/twmb/franz-go v1.22.3 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.20.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.1 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sync v0.25.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.44.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261110000943-09e21b237f2c // indirect
)

replace github.com/palemoky/oppenheimer/pkg => ../../pkg

replace github.com/palemoky/oppenheimer/api => ../../api
