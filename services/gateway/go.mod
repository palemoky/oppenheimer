module github.com/palemoky/Oppenheimer/services/gateway

go 1.27

require (
	github.com/palemoky/Oppenheimer/api v0.0.0
	github.com/palemoky/Oppenheimer/pkg v0.0.0
)

replace github.com/palemoky/Oppenheimer/pkg => ../../pkg

replace github.com/palemoky/Oppenheimer/api => ../../api
