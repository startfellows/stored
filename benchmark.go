package stored

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"time"
)

var memStats runtime.MemStats

type benchmarkResult struct {
	Err      error
	Queries  int
	Duration time.Duration
	Allocs   uint64
	Bytes    uint64
}

func (res *benchmarkResult) QueriesPerSecond() float64 {
	return float64(res.Queries) / res.Duration.Seconds()
}

func (res *benchmarkResult) AllocsPerQuery() int {
	return int(res.Allocs) / res.Queries
}

func (res *benchmarkResult) BytesPerQuery() int {
	return int(res.Bytes) / res.Queries
}

type benchmark struct {
	name string
	n    int
	bm   func(*Cluster, int) error
}

func (b *benchmark) run(db *Cluster) benchmarkResult {
	runtime.GC()

	runtime.ReadMemStats(&memStats)
	var (
		startMallocs    = memStats.Mallocs
		startTotalAlloc = memStats.TotalAlloc
		startTime       = time.Now()
	)

	err := b.bm(db, b.n)

	endTime := time.Now()
	runtime.ReadMemStats(&memStats)

	return benchmarkResult{
		Err:      err,
		Queries:  b.n,
		Duration: endTime.Sub(startTime),
		Allocs:   memStats.Mallocs - startMallocs,
		Bytes:    memStats.TotalAlloc - startTotalAlloc,
	}
}

type benchmarkSuite struct {
	DB          *Cluster
	benchmarks  []benchmark
	WarmUp      func(*Cluster) error
	Repetitions int
	PrintStats  bool

	// Test objects
	testObject *Object
}

func (bs *benchmarkSuite) Init() {
	dir := bs.DB.Directory("benchmark")

	bs.testObject = dir.Object("test_object", benchmarkTestObject{}).Done()
}

func (bs *benchmarkSuite) AddBenchmark(name string, n int, bm func(*Cluster, int) error) {
	bs.benchmarks = append(bs.benchmarks, benchmark{
		name: name,
		n:    n,
		bm:   bm,
	})
}

func (bs *benchmarkSuite) Run() {
	startTime := time.Now()

	if len(bs.benchmarks) < 1 {
		fmt.Println("No benchmark functions registered!")
		return
	}

	if bs.WarmUp != nil {
		fmt.Println("Warming up FoundationDB...")
		if err := bs.WarmUp(bs.DB); err != nil {
			fmt.Println(err.Error())
			return
		}
	}

	fmt.Println("Run Benchmarks...")
	fmt.Println()

	var qps []float64
	if bs.Repetitions > 1 && bs.PrintStats {
		qps = make([]float64, bs.Repetitions)
	} else {
		bs.PrintStats = false
	}

	for _, bm := range bs.benchmarks {
		fmt.Println(bm.name, bm.n, "iterations")
		for i := 0; i < bs.Repetitions; i++ {
			res := bm.run(bs.DB)
			if res.Err != nil {
				fmt.Println(res.Err.Error())
			} else {
				fmt.Println(
					" "+
						res.Duration.String(), "\t   ",
					int(res.QueriesPerSecond()+0.5), "queries/sec\t   ",
					res.AllocsPerQuery(), "allocs/query\t   ",
					res.BytesPerQuery(), "B/query",
				)
				if bs.Repetitions > 1 {
					qps[i] = res.QueriesPerSecond()
				}
			}
		}

		if bs.PrintStats {
			var totalQps float64
			for i := range qps {
				totalQps += qps[i]
			}

			sort.Float64s(qps)

			fmt.Println(
				" -- "+
					"avg", int(totalQps/float64(len(qps))+0.5), "qps;  "+
					"median", int(qps[len(qps)/2]+0.5), "qps",
			)
		}

		fmt.Println()
	}
	endTime := time.Now()
	fmt.Println("Finished... Total running time:", endTime.Sub(startTime).String())
}

// benchmarkWarmup not really necessary, but will be cool to
func benchmarkWarmup(db *Cluster) error {
	for i := 0; i < 1000; i++ {
		_, err := db.Status()
		if err != nil {
			return err
		}
	}

	return nil
}

type benchmarkTestObject struct {
	ID         int64  `stored:"id,primary,autoincrement"`
	Login      string `stored:"login"`
	Name       string `stored:"name"`
	Bio        string `stored:"bio"`
	Token      string `stored:"token"`
	Layers     string `stored:"layers"`
	PhoneHash  string `stored:"phone"`
	FacebookID string `stored:"fb"`
	VkID       string `stored:"vk"`
	Online     int64  `stored:"online,mutable"`
	Ban        bool   `stored:"ban"`
	Woman      bool   `stored:"woman"`
	Followers  int32  `stored:"followers"`
	Following  int32  `stored:"following"`
	Karma      int32  `stored:"karma"`
}

func (o *benchmarkTestObject) Init() {
	randS := func(n int) string {
		s := make([]byte, n/2)
		rand.Read(s)
		return hex.EncodeToString(s)
	}

	o.Login = randS(4096)
	o.Name = randS(4096)
	o.Bio = randS(4096)
	o.Token = randS(4096)
	o.Online = rand.Int63()
	o.Followers = rand.Int31()
	o.Following = rand.Int31()
	o.Karma = rand.Int31()
}

func (bs *benchmarkSuite) bmSimpleWrite(db *Cluster, n int) error {
	dir := db.Directory("benchmark")

	for i := 0; i < n; i++ {
		err := dir.Write(func(tr *Transaction) {
			testObject := &benchmarkTestObject{
				ID: int64(i),
			}
			testObject.Init()

			bs.testObject.Set(testObject).Check(tr)
		}).Err()

		if err != nil {
			return err
		}
	}

	return nil
}

func (bs *benchmarkSuite) bmSimpleRead(db *Cluster, n int) error {
	dir := db.Directory("benchmark")

	for i := 0; i < n; i++ {
		err := dir.Read(func(tr *Transaction) {
			testObject := &benchmarkTestObject{ID: rand.Int63n(25000)}

			bs.testObject.Get(testObject).Check(tr)
		}).Err()

		if err != nil {
			return err
		}
	}

	return nil
}

func (bs *benchmarkSuite) bmSimpleReadWrite(db *Cluster, n int) error {
	dir := db.Directory("benchmark")

	for i := 0; i < n; i++ {
		var err error

		if n%10 == 0 {
			err = dir.Write(func(tr *Transaction) {
				testObject := &benchmarkTestObject{
					ID: rand.Int63n(25000),
				}
				testObject.Init()

				bs.testObject.Set(testObject).Check(tr)
			}).Err()
		} else {
			err = dir.Read(func(tr *Transaction) {
				testObject := &benchmarkTestObject{ID: rand.Int63n(25000)}

				bs.testObject.Get(testObject).Check(tr)
			}).Err()
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (bs *benchmarkSuite) bmConcurrentRead(db *Cluster, n int) error {
	dir := db.Directory("benchmark")

	var wg sync.WaitGroup
	threads := 100

	for c := 0; c < threads; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < n/threads; i++ {
				err := dir.Read(func(tr *Transaction) {
					testObject := &benchmarkTestObject{ID: rand.Int63n(25000)}

					bs.testObject.Get(testObject).Check(tr)
				}).Err()

				if err != nil {
					panic(err)
				}
			}
		}()
	}

	wg.Wait()

	return nil
}

// BenchmarksRun runs benchmarks for STORED FoundationDB layer
func BenchmarksRun(db *Cluster) {
	rand.Seed(time.Now().UnixNano())

	bs := benchmarkSuite{
		DB:          db,
		WarmUp:      benchmarkWarmup,
		Repetitions: 3,
		PrintStats:  true,
	}

	bs.Init()

	bs.AddBenchmark("SimpleWrite", 25000, bs.bmSimpleWrite)
	/*bs.AddBenchmark("SimpleRead", 250000, bs.bmSimpleRead)
	bs.AddBenchmark("SimpleReadWrite", 25000, bs.bmSimpleReadWrite)
	bs.AddBenchmark("ConcurrentRead", 250000, bs.bmConcurrentRead)*/

	bs.Run()
}
