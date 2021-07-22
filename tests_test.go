package stored

import (
	"os"
	"testing"
)

func TestStored(t *testing.T) {
	dbDriver := Connect(os.Getenv("FDB_CLUSTER_FILE"))

	TestsRun(dbDriver)
}
