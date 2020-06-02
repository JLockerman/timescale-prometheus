package pgclient

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
	"github.com/timescale/timescale-prometheus/pkg/prompb"

	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/util"
)

// Config for the database
type Config struct {
	host             string
	port             int
	user             string
	password         string
	database         string
	sslMode          string
	dbConnectRetries int
	AsyncAcks        bool
	ReportInterval   int
	NumInserters     int
	BatchSize        int
}

// ParseFlags parses the configuration flags specific to PostgreSQL and TimescaleDB
func ParseFlags(cfg *Config) *Config {
	flag.StringVar(&cfg.host, "db-host", "localhost", "The TimescaleDB host")
	flag.IntVar(&cfg.port, "db-port", 5432, "The TimescaleDB port")
	flag.StringVar(&cfg.user, "db-user", "postgres", "The TimescaleDB user")
	flag.StringVar(&cfg.password, "db-password", "", "The TimescaleDB password")
	flag.StringVar(&cfg.database, "db-name", "timescale", "The TimescaleDB database")
	flag.StringVar(&cfg.sslMode, "db-ssl-mode", "disable", "The TimescaleDB connection ssl mode")
	flag.IntVar(&cfg.dbConnectRetries, "db-connect-retries", 0, "How many times to retry connecting to the database")
	flag.BoolVar(&cfg.AsyncAcks, "async-acks", false, "Ack before data is written to DB")
	flag.IntVar(&cfg.ReportInterval, "tput-report", 0, "interval in seconds at which throughput should be reported")
	flag.IntVar(&cfg.NumInserters, "inserters", 1, "number of workers inserting concurrently")
	flag.IntVar(&cfg.BatchSize, "batch-size", 1, "how much to insert at once")
	return cfg
}

// Client sends Prometheus samples to TimescaleDB
type Client struct {
	Connection    *pgxpool.Pool
	cfg           *Config
	ConnectionStr string
	counters      []int64
}

// NewClient creates a new PostgreSQL client
func NewClient(cfg *Config) (*Client, error) {
	connectionStr := cfg.GetConnectionStr()

	maxProcs := runtime.GOMAXPROCS(-1)
	if maxProcs <= 0 {
		maxProcs = runtime.NumCPU()
	}
	if maxProcs <= 0 {
		maxProcs = 1
	}
	connectionPool, err := pgxpool.Connect(context.Background(), connectionStr+fmt.Sprintf(" pool_max_conns=%d pool_min_conns=%d", cfg.NumInserters, cfg.NumInserters))

	log.Info("msg", util.MaskPassword(connectionStr))

	if err != nil {
		log.Error("err creating connection pool for new client", util.MaskPassword(err.Error()))
		return nil, err
	}

	counters := make([]int64, cfg.NumInserters)

	return &Client{Connection: connectionPool, cfg: cfg, counters: counters}, nil
}

// GetConnectionStr returns a Postgres connection string
func (cfg *Config) GetConnectionStr() string {
	return fmt.Sprintf("host=%v port=%v user=%v dbname=%v password='%v' sslmode=%v connect_timeout=10",
		cfg.host, cfg.port, cfg.user, cfg.database, cfg.password, cfg.sslMode)
}

func (c *Client) Run() {
	connection := pgmodel.NewPgxConnImpl(c.Connection)
	conn := &connection
	batchSize := c.cfg.BatchSize
	allReady := &sync.WaitGroup{}
	allReady.Add(len(c.counters) + 1)
	for i := 0; i < len(c.counters); i++ {
		myIdx := i
		go func() {
			iter := pgmodel.NewSampleInfoIterator()
			samples := pgmodel.SamplesInfo{SeriesID: pgmodel.SeriesID(myIdx), Samples: make([]prompb.Sample, batchSize)}
			iter.Append(samples)
			seriesStartTime := time.Date(2100, 3, 3, 1, 1, 0, 0, time.UTC)
			tableName, _, err := pgmodel.GetMetricTableName(conn, fmt.Sprintf("metric%d", myIdx))
			if err != nil {
				panic(err)
			}
			// startTimestamp := prompb.Time
			allReady.Done()
			allReady.Wait()
			for iters := 0; true; iters++ {
				for i := range samples.Samples {
					samples.Samples[i] = prompb.Sample{
						Value:     1.0,
						Timestamp: seriesStartTime.Add(time.Duration(i*178+iters*batchSize) * time.Microsecond),
					}
				}
				pgmodel.RunCopyFrom(conn, &iter, tableName)
				atomic.AddInt64(&c.counters[myIdx], int64(batchSize))
				iter.ResetPosition()
			}
		}()
	}

	prevCounters := make([]int64, len(c.counters))
	allReady.Done()
	allReady.Wait()
	tick := time.NewTicker(time.Duration(c.cfg.ReportInterval) * time.Second)
	start := time.Now()
	fmt.Printf("batch size: %6d\n   workers: %6d\n", c.cfg.BatchSize, c.cfg.NumInserters)
	fmt.Printf(" %7s, %10s, %10s\n", "elapsed", "samples/s", "overall samples/s")
	for {
		<-tick.C
		elapsed := int64(time.Since(start).Seconds())
		totalSentThisTick := int64(0)
		totalSent := int64(0)
		for i := range c.counters {
			total := atomic.LoadInt64(&c.counters[i])
			sentThisTick := total - prevCounters[i]
			prevCounters[i] = total
			totalSentThisTick += sentThisTick
			totalSent += total
		}
		rateThisTick := totalSentThisTick / int64(c.cfg.ReportInterval)
		overallRate := totalSent / elapsed
		fmt.Printf("%7ds, %10d, %10d\n", elapsed, rateThisTick, overallRate)
	}
}

// Close closes the client and performs cleanup
func (c *Client) Close() {
	c.Connection.Close()
}
