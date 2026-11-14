module github.com/palemoky/oppenheimer/services/recon

go 1.27

require (
	github.com/palemoky/oppenheimer/pkg v0.0.0
	github.com/palemoky/oppenheimer/services/inventory v0.0.0
	github.com/palemoky/oppenheimer/services/order v0.0.0
	github.com/palemoky/oppenheimer/services/orderindex v0.0.0
	github.com/palemoky/oppenheimer/services/payment v0.0.0
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/XSAM/otelsql v0.44.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-sql-driver/mysql v1.10.1 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/pressly/goose/v3 v3.30.0 // indirect
	github.com/sethvargo/go-retry v0.5.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/otel/trace v1.47.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sync v0.25.0 // indirect
)

replace (
	github.com/palemoky/oppenheimer/api => ../../api
	github.com/palemoky/oppenheimer/pkg => ../../pkg
	github.com/palemoky/oppenheimer/services/inventory => ../inventory
	github.com/palemoky/oppenheimer/services/order => ../order
	github.com/palemoky/oppenheimer/services/orderindex => ../orderindex
	github.com/palemoky/oppenheimer/services/payment => ../payment
)
