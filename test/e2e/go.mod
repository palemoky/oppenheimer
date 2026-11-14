module github.com/palemoky/oppenheimer/test/e2e

go 1.27

require github.com/palemoky/oppenheimer/pkg v0.0.0

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/go-sql-driver/mysql v1.10.1 // indirect
	github.com/mfridman/interpolate v0.0.2 // indirect
	github.com/pressly/goose/v3 v3.30.0 // indirect
	github.com/sethvargo/go-retry v0.5.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sync v0.25.0 // indirect
)

replace github.com/palemoky/oppenheimer/pkg => ../../pkg
