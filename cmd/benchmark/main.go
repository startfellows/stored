package main

import (
	"github.com/swork9/stored"
	"os"
)

func main() {
	dbDriver := stored.Connect(os.Getenv("FDB_CLUSTER_FILE"))

	stored.BenchmarksRun(dbDriver)
}
