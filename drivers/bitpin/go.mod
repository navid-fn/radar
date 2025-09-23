module github.com/navid-fn/radar

go 1.24.5

require (
	github.com/confluentinc/confluent-kafka-go/v2 v2.11.1
	github.com/gorilla/websocket v1.5.3
	github.com/navid-fn/radar/pkg v0.0.0-00010101000000-000000000000
	github.com/sirupsen/logrus v1.9.3
)

require golang.org/x/sys v0.25.0 // indirect

replace github.com/navid-fn/radar/pkg => ../pkg
