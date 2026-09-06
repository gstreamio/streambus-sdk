package transaction

import (
	"os"

	"github.com/gstreamio/streambus-sdk/logging"
)

// testLogger mirrors the helper in the core repository's coordinator_test.go,
// which this SDK does not vendor because it imports testify.
func testLogger() *logging.Logger {
	return logging.New(&logging.Config{
		Level:  logging.LevelDebug,
		Output: os.Stdout,
	})
}
