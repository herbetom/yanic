package database

import (
	"strings"
	"time"
	"os"

	"github.com/FreifunkBremen/yanic/runtime"
)

// Connection interface to use for implementation in e.g. influxdb
type Connection interface {
	// InsertNode stores statistics per node
	InsertNode(node *runtime.Node)

	// InsertLink stores statistics per link
	InsertLink(*runtime.Link, time.Time)

	// InsertGlobals stores global statistics
	InsertGlobals(*runtime.GlobalStats, time.Time, string, string)

	// PruneNodes prunes historical per-node data
	PruneNodes(deleteAfter time.Duration)

	// Close closes the database connection
	Close()
}

// Connect function with config to get DB connection interface
type Connect func(config map[string]interface{}) (Connection, error)

// Adapters is the list of registered database adapters
var Adapters = map[string]Connect{}

func RegisterAdapter(name string, n Connect) {
	Adapters[name] = n
}

func LoadCredentialFromFile(filename string) string {
	// Expand environment variables in the password file path. This for example enables the use of systemd credentials
	expandedPath := os.ExpandEnv(filename)

	file, err := os.ReadFile(expandedPath)
	if err != nil {
		panic(err)
	}

	// Remove all leading and trailing white spaces from the password and return it
	return strings.TrimSpace(string(file))
}