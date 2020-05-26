// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/allegro/bigcache"
	"github.com/google/btree"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgio"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/timescale/timescale-prometheus/pkg/log"
	"github.com/timescale/timescale-prometheus/pkg/prompb"

	"github.com/timescale/timescale-prometheus/pkg/compression"
)

const (
	promSchema       = "prom_api"
	seriesViewSchema = "prom_series"
	metricViewSchema = "prom_metric"
	dataSchema       = "prom_data"
	dataSeriesSchema = "prom_data_series"
	infoSchema       = "prom_info"
	catalogSchema    = "_prom_catalog"
	extSchema        = "_prom_ext"

	getMetricsTableSQL              = "SELECT table_name FROM " + catalogSchema + ".get_metric_table_name_if_exists($1)"
	getCreateMetricsTableSQL        = "SELECT table_name FROM " + catalogSchema + ".get_or_create_metric_table_name($1)"
	getCreateMetricsTableWithNewSQL = "SELECT table_name, possibly_new FROM " + catalogSchema + ".get_or_create_metric_table_name($1)"
	finalizeMetricCreation          = "CALL " + catalogSchema + ".finalize_metric_creation()"
	getSeriesIDForLabelSQL          = "SELECT * FROM " + catalogSchema + ".get_series_id_for_key_value_array($1, $2, $3)"
	ensureChunksSql                 = "SELECT * FROM " + catalogSchema + ".ensure_compressed_chunks('" + dataSchema + "', $1, $2)"
)

var (
	copyColumns         = []string{"time", "value", "series_id"}
	errMissingTableName = fmt.Errorf("missing metric table name")
)

const (
	// postgres time zero is Sat Jan 01 00:00:00 2000 UTC
	// this is the offset of the unix epoch in microseconds from the postgres zero
	PostgresUnixEpoch = -946684800000000
	microsPerMs       = 1000
)

var (
	postgresTimeInf    = Timestamptz{math.MaxInt64}
	postgresTimeNegInf = Timestamptz{math.MinInt64}
)

func ToPostgresTime(tm model.Time) (pgTime Timestamptz, isInf bool) {
	// this code is essentially
	//     pgTime = int64(tm)*microsPerMs + PostgresUnixEpoch
	// except with oveflow checks
	ms := int64(tm)
	if ms <= math.MaxInt64/microsPerMs {
		ms *= microsPerMs
		if ms > postgresTimeNegInf.Time-PostgresUnixEpoch {
			return Timestamptz{ms + PostgresUnixEpoch}, false
		}

		return postgresTimeNegInf, true
	}
	// the PostgresUnixEpoch is negative, so maybe we can shrink the time
	// into range? (ms must be positive here)
	ms += PostgresUnixEpoch / microsPerMs
	if ms < math.MaxInt64/microsPerMs {
		return Timestamptz{ms * microsPerMs}, false
	}

	return postgresTimeInf, true
}

func FromPostgresTime(time Timestamptz) (promTime model.Time, isInf bool) {
	if time == postgresTimeInf {
		return model.Latest, true
	} else if time == postgresTimeNegInf {
		return model.Earliest, true
	}

	micros := time.Time
	// this code is essentially
	//   model.Time(tm/microsPerMs - PostgresUnixEpoch/microsPerMs)
	// except with overflow checks
	if micros < postgresTimeInf.Time+PostgresUnixEpoch {
		return model.Time((micros - PostgresUnixEpoch) / microsPerMs), false
	}

	// maybe we can shrink it
	ms := micros / microsPerMs
	msEpoch := int64(PostgresUnixEpoch) / microsPerMs
	if ms < math.MaxInt64+msEpoch {
		return model.Time(ms - msEpoch), false
	}

	return model.Latest, true
}

type pgxBatch interface {
	Queue(query string, arguments ...interface{})
}

type pgxConn interface {
	Close()
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	CopyFromRows(rows [][]interface{}) pgx.CopyFromSource
	NewBatch() pgxBatch
	SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error)
}

// MetricCache provides a caching mechanism for metric table names.
type MetricCache interface {
	Get(metric string) (string, error)
	Set(metric string, tableName string) error
}

type pgxConnImpl struct {
	conn *pgxpool.Pool
}

func (p *pgxConnImpl) getConn() *pgxpool.Pool {
	return p.conn
}

func (p *pgxConnImpl) Close() {
	conn := p.getConn()
	p.conn = nil
	conn.Close()
}

func (p *pgxConnImpl) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	conn := p.getConn()

	return conn.Exec(ctx, sql, arguments...)
}

func (p *pgxConnImpl) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	conn := p.getConn()

	return conn.Query(ctx, sql, args...)
}

func (p *pgxConnImpl) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	conn := p.getConn()

	return conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *pgxConnImpl) CopyFromRows(rows [][]interface{}) pgx.CopyFromSource {
	return pgx.CopyFromRows(rows)
}

func (p *pgxConnImpl) NewBatch() pgxBatch {
	return &pgx.Batch{}
}

func (p *pgxConnImpl) SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error) {
	conn := p.getConn()

	return conn.SendBatch(ctx, b.(*pgx.Batch)), nil
}

// SampleInfoIterator is an iterator over a collection of sampleInfos that returns
// data in the format expected for the data table row.
type SampleInfoIterator struct {
	sampleInfos     []samplesInfo
	sampleInfoIndex int
	sampleIndex     int
}

// NewSampleInfoIterator is the constructor
func NewSampleInfoIterator() SampleInfoIterator {
	return SampleInfoIterator{sampleInfos: make([]samplesInfo, 0), sampleIndex: -1, sampleInfoIndex: 0}
}

//Append adds a sample info to the back of the iterator
func (t *SampleInfoIterator) Append(s samplesInfo) {
	t.sampleInfos = append(t.sampleInfos, s)
}

// Next returns true if there is another row and makes the next row data
// available to Values(). When there are no more rows available or an error
// has occurred it returns false.
func (t *SampleInfoIterator) Next() bool {
	t.sampleIndex++
	if t.sampleInfoIndex < len(t.sampleInfos) && t.sampleIndex >= len(t.sampleInfos[t.sampleInfoIndex].samples) {
		t.sampleInfoIndex++
		t.sampleIndex = 0
	}
	return t.sampleInfoIndex < len(t.sampleInfos)
}

// Values returns the values for the current row
func (t *SampleInfoIterator) Values() ([]interface{}, error) {
	info := t.sampleInfos[t.sampleInfoIndex]
	sample := info.samples[t.sampleIndex]
	row := []interface{}{
		model.Time(sample.Timestamp).Time(),
		sample.Value,
		info.seriesID,
	}
	return row, nil
}

// Err returns any error that has been encountered by the CopyFromSource. If
// this is not nil *Conn.CopyFrom will abort the copy.
func (t *SampleInfoIterator) Err() error {
	return nil
}

// NewPgxIngestorWithMetricCache returns a new Ingestor that uses connection pool and a metrics cache
// for caching metric table names.
func NewPgxIngestorWithMetricCache(c *pgxpool.Pool, cache MetricCache, asyncAcks bool) (*DBIngestor, error) {

	conn := &pgxConnImpl{
		conn: c,
	}

	pi, err := newPgxInserter(conn, cache, asyncAcks)
	if err != nil {
		return nil, err
	}

	series, _ := bigcache.NewBigCache(DefaultCacheConfig())

	bc := &bCache{
		series: series,
	}

	return &DBIngestor{
		db:    pi,
		cache: bc,
	}, nil
}

// NewPgxIngestor returns a new Ingestor that write to PostgreSQL using PGX
func NewPgxIngestor(c *pgxpool.Pool) (*DBIngestor, error) {
	metrics, _ := bigcache.NewBigCache(DefaultCacheConfig())
	cache := &MetricNameCache{metrics}
	return NewPgxIngestorWithMetricCache(c, cache, false)
}

func newPgxInserter(conn pgxConn, cache MetricCache, asyncAcks bool) (*pgxInserter, error) {
	cmc := make(chan struct{}, 1)

	inserter := &pgxInserter{
		conn:                   conn,
		metricTableNames:       cache,
		completeMetricCreation: cmc,
		asyncAcks:              asyncAcks,
	}
	if asyncAcks {
		var dataPoints uint64
		inserter.insertedDatapoints = &dataPoints
		go func() {
			log.Info("msg", "outputting throughpput info once every 10s")
			tick := time.Tick(10 * time.Second)
			for range tick {
				inserted := atomic.SwapUint64(inserter.insertedDatapoints, 0)
				log.Info("msg", "Samples write throughput", "samples/sec", inserted/10)
			}
		}()
	}
	//on startup run a completeMetricCreation to recover any potentially
	//incomplete metric
	err := inserter.CompleteMetricCreation()
	if err != nil {
		return nil, err
	}

	go inserter.runCompleteMetricCreationWorker()

	return inserter, nil
}

type pgxInserter struct {
	conn                   pgxConn
	metricTableNames       MetricCache
	inserters              sync.Map
	completeMetricCreation chan struct{}
	asyncAcks              bool
	insertedDatapoints     *uint64
}

func (p *pgxInserter) CompleteMetricCreation() error {
	_, err := p.conn.Exec(
		context.Background(),
		finalizeMetricCreation,
	)
	return err
}

func (p *pgxInserter) runCompleteMetricCreationWorker() {
	for range p.completeMetricCreation {
		err := p.CompleteMetricCreation()
		if err != nil {
			log.Warn("Got an error finalizing metric: %v", err)
		}
	}
}

func (p *pgxInserter) Close() {
	close(p.completeMetricCreation)
	p.inserters.Range(func(key, value interface{}) bool {
		close(value.(chan insertDataRequest))
		return true
	})
}

func (p *pgxInserter) InsertNewData(rows map[string][]samplesInfo, ctx *InsertCtx) (uint64, error) {
	return p.InsertData(rows, ctx)
}

type insertDataRequest struct {
	metric   string
	data     []samplesInfo
	finished *sync.WaitGroup
	errChan  chan error
	ctx      *InsertCtx
}

type insertDataTask struct {
	finished *sync.WaitGroup
	errChan  chan error
}

func (p *pgxInserter) InsertData(rows map[string][]samplesInfo, ctx *InsertCtx) (uint64, error) {
	var numRows uint64
	workFinished := &sync.WaitGroup{}
	workFinished.Add(len(rows))
	ctx.refs = int64(len(rows))
	errChan := make(chan error, 1)
	for metricName, data := range rows {
		for _, si := range data {
			numRows += uint64(len(si.samples))
		}
		p.insertMetricData(metricName, data, workFinished, errChan, ctx)
	}

	if !p.asyncAcks {
		workFinished.Wait()
		var err error
		select {
		case err = <-errChan:
		default:
		}
		close(errChan)
		return numRows, err
	} else {
		go func() {
			workFinished.Wait()
			var err error
			select {
			case err = <-errChan:
			default:
			}
			close(errChan)
			if err != nil {
				log.Error("msg", "error on send", "error", err)
			} else {
				atomic.AddUint64(p.insertedDatapoints, numRows)
			}
		}()
	}
	return numRows, nil
}

func (p *pgxInserter) insertMetricData(metric string, data []samplesInfo, finished *sync.WaitGroup, errChan chan error, ctx *InsertCtx) {
	inserter := p.getMetricInserter(metric, errChan)
	inserter <- insertDataRequest{metric: metric, data: data, finished: finished, errChan: errChan, ctx: ctx}
}

func (p *pgxInserter) getMetricInserter(metric string, errChan chan error) chan insertDataRequest {
	inserter, ok := p.inserters.Load(metric)
	if !ok {
		c := make(chan insertDataRequest, 10000)
		actual, old := p.inserters.LoadOrStore(metric, c)
		inserter = actual
		if !old {
			go runInserterRoutine(p.conn, c, metric, p.completeMetricCreation, errChan, p.metricTableNames)
		}
	}
	return inserter.(chan insertDataRequest)
}

func (p *pgxInserter) createMetricTable(metric string) (string, error) {
	res, err := p.conn.Query(
		context.Background(),
		getCreateMetricsTableSQL,
		metric,
	)

	if err != nil {
		return "", err
	}

	var tableName string
	defer res.Close()
	if !res.Next() {
		return "", errMissingTableName
	}

	if err := res.Scan(&tableName); err != nil {
		return "", err
	}

	return tableName, nil
}

func (p *pgxInserter) getMetricTableName(metric string) (string, error) {
	var err error
	var tableName string

	tableName, err = p.metricTableNames.Get(metric)

	if err == nil {
		return tableName, nil
	}

	if err != ErrEntryNotFound {
		return "", err
	}

	tableName, err = p.createMetricTable(metric)

	if err != nil {
		return "", err
	}

	err = p.metricTableNames.Set(metric, tableName)

	return tableName, err
}

type insertHandler struct {
	conn            pgxConn
	input           chan insertDataRequest
	seriesCache     map[string]SeriesID
	metricTableName string
	chunks          *btree.BTree // stores *chunkInfo
	timeout         *time.Timer
	timoutSet       bool
	samplesPending  int
}

type chunkInfo struct {
	name      string
	startTime Timestamptz
	endTime   Timestamptz
	pending   map[string]*seriesInfo
}

type seriesInsert struct {
	table string
	data  seriesInfo
}

type seriesInfo struct {
	id      SeriesID
	pending pendingBuffer
	lastTs  Timestamptz
	lastSeq int32
}

type pendingBuffer struct {
	timestamps   []Timestamptz
	dataPoints   []float64
	creationTime time.Time
	notify       []*finishedNotify
}

type finishedNotify struct {
	onFinished   *sync.WaitGroup
	numFragments int
}

type Timestamptz struct {
	Time int64
}

func (tm Timestamptz) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	return pgio.AppendInt64(buf, tm.Time), nil
}

func (dst *Timestamptz) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		panic("shouldn't have NULL timestamp")
	}

	if len(src) != 8 {
		return fmt.Errorf("invalid length for timestamptz: %v", len(src))
	}

	dst.Time = int64(binary.BigEndian.Uint64(src))
	return nil
}

type TimestamptzArray struct {
	Values []Timestamptz
}

func (src TimestamptzArray) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	arrayHeader := pgtype.ArrayHeader{
		Dimensions:   []pgtype.ArrayDimension{{Length: int32(len(src.Values)), LowerBound: 1}},
		ContainsNull: false,
	}

	if dt, ok := ci.DataTypeForName("timestamptz"); ok {
		arrayHeader.ElementOID = int32(dt.OID)
	} else {
		return nil, errors.Errorf("unable to find oid for type name %v", "timestamptz")
	}

	buf = arrayHeader.EncodeBinary(ci, buf)

	for i := range src.Values {
		sp := len(buf)
		buf = pgio.AppendInt32(buf, -1)

		elemBuf, err := src.Values[i].EncodeBinary(ci, buf)
		if err != nil {
			return nil, err
		}
		if elemBuf != nil {
			buf = elemBuf
			pgio.SetInt32(buf[sp:], int32(len(buf[sp:])-4))
		}
	}

	return buf, nil
}

type rawbinary struct {
	bytes []byte
}

func (src rawbinary) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	return append(buf, src.bytes...), nil
}

func (dst *TimestamptzArray) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		panic("should not have NULL")
	}

	var arrayHeader pgtype.ArrayHeader
	rp, err := arrayHeader.DecodeBinary(ci, src)
	if err != nil {
		return err
	}

	if len(arrayHeader.Dimensions) != 1 {
		return nil
	}

	elementCount := arrayHeader.Dimensions[0].Length
	for _, d := range arrayHeader.Dimensions[1:] {
		elementCount *= d.Length
	}

	elements := make([]Timestamptz, elementCount)

	for i := range elements {
		elemLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4
		var elemSrc []byte
		if elemLen >= 0 {
			elemSrc = src[rp : rp+elemLen]
			rp += elemLen
		}
		err = elements[i].DecodeBinary(ci, elemSrc)
		if err != nil {
			return err
		}
	}

	*dst = TimestamptzArray{Values: elements}
	return nil
}

const (
	flushSize    = 2000
	flushTimeout = 1000 * time.Millisecond
)

func getMetricTableName(conn pgxConn, metric string) (string, bool, error) {
	res, err := conn.Query(
		context.Background(),
		getCreateMetricsTableWithNewSQL,
		metric,
	)

	if err != nil {
		return "", true, err
	}

	var tableName string
	var possiblyNew bool
	defer res.Close()
	if !res.Next() {
		return "", true, errMissingTableName
	}

	if err := res.Scan(&tableName, &possiblyNew); err != nil {
		return "", true, err
	}

	return tableName, possiblyNew, nil
}

func runInserterRoutineFailure(input chan insertDataRequest, err error) {
	for idr := range input {
		select {
		case idr.errChan <- fmt.Errorf("The insert routine has previously failed with %w", err):
		default:
		}
		idr.finished.Done()
	}
}

func runInserterRoutine(conn pgxConn, input chan insertDataRequest, metricName string, completeMetricCreationSignal chan struct{}, errChan chan error, metricTableNames MetricCache) {
	tableName, err := metricTableNames.Get(metricName)
	if err == ErrEntryNotFound {
		var possiblyNew bool
		tableName, possiblyNew, err = getMetricTableName(conn, metricName)
		if err != nil {
			select {
			case errChan <- err:
			default:
			}
			//won't be able to insert anyway
			runInserterRoutineFailure(input, err)
			return
		} else {
			//ignone error since this is just an optimization
			_ = metricTableNames.Set(metricName, tableName)
		}

		if possiblyNew {
			//pass a signal if there is space
			select {
			case completeMetricCreationSignal <- struct{}{}:
			default:
			}
		}
	} else if err != nil {
		if err != nil {
			select {
			case errChan <- err:
			default:
			}
		}
		//won't be able to insert anyway
		runInserterRoutineFailure(input, err)
		return
	}

	handler := insertHandler{
		conn:            conn,
		input:           input,
		chunks:          btree.New(8),
		seriesCache:     make(map[string]SeriesID),
		metricTableName: tableName,
		timeout:         time.NewTimer(flushTimeout),
		timoutSet:       true,
	}

	for {

		select {
		case req, ok := <-handler.input:
			if !ok {
				return
			}
			handler.handleReq(req)
		case <-handler.timeout.C:
			handler.timoutSet = false
			for handler.flushTimedOutBatches() {
			hot:
				for i := 0; i < 1000; i++ {
					select {
					case req := <-handler.input:
						handler.handleReq(req)
					default:
						break hot
					}
				}
			}
		}
		if handler.hasPendingReqs() && !handler.timoutSet {
			handler.timeout.Reset(flushTimeout)
			handler.timoutSet = true
		}
	}
}

func (h *insertHandler) hasPendingReqs() bool {
	return h.samplesPending > 0
}

func (h *insertHandler) handleReq(req insertDataRequest) {
	//TODO I think we'll eventually want to batch different kinds of DB reqs together
	h.setSeriesIds(&req)

	onFinish := &finishedNotify{
		//TODO error chan?
		onFinished:   req.finished,
		numFragments: 1,
	}
	missing, needsFlush := h.addSamples(req.data, onFinish)
	if missing != nil {
		h.ensureChunksFor(missing)
		_, moreFlush := h.addSamples(req.data, onFinish)
		needsFlush = append(needsFlush, moreFlush...)
	}
	req.ctx.Close()

	onFinish.finishedFragment()

	if needsFlush != nil {
		h.compressAndflush(needsFlush)
	}
}

func (h *insertHandler) flushTimedOutBatches() bool {
	numToDelete := 0
	var toFlush []seriesInsert
	now := time.Now()
	deleteOlderThan, _ := ToPostgresTime(model.TimeFromUnix(now.Add(-7 * 24 * time.Hour).Unix()))
	canDeleteMore := true
	hasMoreTimedOut := false
	h.chunks.Ascend(func(i btree.Item) bool {
		empty := true
		chunk := i.(*chunkInfo)
		for k, series := range chunk.pending {
			if series.pending.Len() == 0 {
				continue
			}
			if now.Sub(series.pending.creationTime) >= flushTimeout {
				// if len(toFlush) < 10 {
				// 	toFlush = append(toFlush, chunk.takeSeries(k, series))
				// } else {
				// 	hasMoreTimedOut = true
				// 	empty = false
				// }
				toFlush = append(toFlush, chunk.takeSeries(k, series))
			} else {
				empty = false
			}
		}
		if !empty {
			canDeleteMore = false
		}
		if canDeleteMore && chunk.endTime.Less(deleteOlderThan) {
			numToDelete += 1
		}
		return !hasMoreTimedOut
	})

	for i := 0; i < numToDelete; i++ {
		h.chunks.DeleteMin()
	}

	// if hasMoreTimedOut && len(toFlush) == 0 {
	// 	panic("not flushing")
	// }

	if len(toFlush) > 0 {
		h.compressAndflush(toFlush)
	}

	return hasMoreTimedOut
}

func (h *insertHandler) addSamples(samples []samplesInfo, onFinish *finishedNotify) (missingChunks map[Timestamptz]struct{}, needsFlush []seriesInsert) {
	for d, data := range samples {
		ensureSamplesAreSorted(data.samples)

		var chunk *chunkInfo
		i := 0
		var samplesInMisingChunks []prompb.Sample
		for i < len(data.samples) {
			sample := data.samples[i]
			timestamp, _ := ToPostgresTime(model.Time(sample.Timestamp))
			if chunk == nil || chunk.endTime.Time <= timestamp.Time {
				chunk = h.getChunkInfoFor(timestamp)
			}

			if chunk == nil {
				if missingChunks == nil {
					missingChunks = make(map[Timestamptz]struct{})
				}
				missingChunks[timestamp] = struct{}{}
				samplesInMisingChunks = append(samplesInMisingChunks, sample)
				i += 1
				continue
			}

			//TODO if we put the series map on the ourside, we only have to look once
			series := chunk.getSeriesInfoFor(data.labels, data.seriesID)
			samplesAdded := series.pending.mergeInSamples(chunk.endTime, data.samples[i:])

			series.pending.notify = append(series.pending.notify, onFinish)
			onFinish.numFragments += 1

			h.samplesPending += samplesAdded
			i += samplesAdded

			if len(series.pending.timestamps) > flushSize {
				toFlush := chunk.takeSeries(data.labels.String(), series)
				needsFlush = append(needsFlush, toFlush)
			}
		}
		samples[d].samples = samplesInMisingChunks
	}
	return
}

func ensureSamplesAreSorted(samples []prompb.Sample) {
	if !sort.SliceIsSorted(samples, func(i, j int) bool {
		return samples[i].Timestamp < samples[j].Timestamp
	}) {
		sort.Slice(samples, func(i, j int) bool {
			return samples[i].Timestamp < samples[j].Timestamp
		})
	}
}

func (h *insertHandler) getChunkInfoFor(time Timestamptz) (chunk *chunkInfo) {
	h.chunks.AscendGreaterOrEqual(time, func(i btree.Item) bool {
		info := i.(*chunkInfo)
		if info.startTime.Time <= time.Time {
			chunk = info
		}
		return false
	})
	return
}

func (c *chunkInfo) getSeriesInfoFor(series Labels, id SeriesID) *seriesInfo {
	info, ok := c.pending[series.String()]
	if !ok {
		info = &seriesInfo{
			id:     id,
			lastTs: postgresTimeNegInf,
			pending: pendingBuffer{
				creationTime: time.Now(),
			},
		}
		c.pending[series.String()] = info
	} else if info.pending.Len() == 0 {
		info.pending.creationTime = time.Now()
	}
	return info
}

func (c *chunkInfo) takeSeries(labels string, series *seriesInfo) seriesInsert {
	if series.lastTs.Time > series.pending.timestamps[0].Time {
		panic("TODO OoO inserts not yet implemented")
	}

	series.lastTs = series.pending.timestamps[len(series.pending.timestamps)-1]
	series.lastSeq += 100

	ret := *series
	series.pending = pendingBuffer{}
	return seriesInsert{c.name, ret}
}

func (p *pendingBuffer) mergeInSamples(endTime Timestamptz, samples []prompb.Sample) int {
	firstSampleTime, _ := ToPostgresTime(model.Time(samples[0].Timestamp))
	if p.Len() == 0 || firstSampleTime.Time >= p.timestamps[len(p.timestamps)-1].Time {
		// all the new times are greater than the old times
		i := 0
		for _, sample := range samples {
			time, _ := ToPostgresTime(model.Time(sample.Timestamp))
			if time.Time >= endTime.Time {
				break
			}
			p.timestamps = append(p.timestamps, time)
			p.dataPoints = append(p.dataPoints, sample.Value)
			i += 1
		}
		return i
	}
	// TODO switch to linear merge algo
	i := 0
	for _, sample := range samples {
		time, _ := ToPostgresTime(model.Time(sample.Timestamp))
		if time.Time >= endTime.Time {
			break
		}
		p.timestamps = append(p.timestamps, time)
		p.dataPoints = append(p.dataPoints, sample.Value)
		i += 1
	}
	sort.Sort(p)
	return i
}

func (p *pendingBuffer) Len() int {
	return len(p.timestamps)
}

func (p *pendingBuffer) Less(i, j int) bool {
	return p.timestamps[i].Time < p.timestamps[j].Time
}

func (p *pendingBuffer) Swap(i, j int) {
	p.timestamps[i], p.timestamps[j] = p.timestamps[j], p.timestamps[i]
	p.dataPoints[i], p.dataPoints[j] = p.dataPoints[j], p.dataPoints[i]
}

func (h *insertHandler) ensureChunksFor(times map[Timestamptz]struct{}) {
	timestamps := make([]Timestamptz, 0, len(times))
	for t := range times {
		timestamps = append(timestamps, t)
	}
	rows, err := h.conn.Query(context.Background(), ensureChunksSql, h.metricTableName, TimestamptzArray{timestamps})
	if err != nil {
		//TODO
		panic(err)
	}
	for rows.Next() {
		var chunkNames []string
		var chunkStartA TimestamptzArray
		var chunkEndA TimestamptzArray
		err = rows.Scan(&chunkNames, &chunkStartA, &chunkEndA)
		if err != nil {
			//TODO
			panic(err)
		}
		chunkStart := chunkStartA.Values
		chunkNames = chunkNames[:len(chunkStart)]
		chunkEnd := chunkEndA.Values[:len(chunkStart)]
		for i := 0; i < len(chunkStart); i++ {
			chunk := &chunkInfo{
				name:      chunkNames[i],
				startTime: chunkStart[i],
				endTime:   chunkEnd[i],
				pending:   make(map[string]*seriesInfo),
			}
			old := h.chunks.ReplaceOrInsert(chunk)
			if old != nil {
				panic(old)
			}
		}
	}
}

func (h *insertHandler) compressAndflush(inserts []seriesInsert) error {
	if inserts == nil {
		return nil
	}
	batch := h.conn.NewBatch()
	for _, insert := range inserts {
		h.samplesPending -= len(insert.data.pending.timestamps)
		// fmt.Println(len(insert.data.pending.timestamps))
		times, data, count, seqNum, minTime, maxTime := insert.data.prepareInsertRow()
		batch.Queue("BEGIN;")
		batch.Queue(fmt.Sprintf("INSERT INTO %s(time, value, series_id, _ts_meta_count, _ts_meta_sequence_num, _ts_meta_min_1, _ts_meta_max_1) VALUES ($1, $2, $3, $4, $5, $6, $7)", insert.table), times, data, insert.data.id, count, seqNum, minTime, maxTime)
		batch.Queue("COMMIT;")
	}

	br, err := h.conn.SendBatch(context.Background(), batch)
	if err != nil {
		//TODO
		panic(err)
	}
	defer br.Close()

	for i := 0; i < len(inserts); i++ {
		_, err = br.Exec()
		if err != nil {
			//TODO
			panic(err)
		}
		_, err = br.Exec()
		if err != nil {
			//TODO
			panic(err)
		}
		_, err = br.Exec()
		if err != nil {
			//TODO
			panic(err)
		}
	}

	for _, insert := range inserts {
		insert.data.finishedFragment()
	}

	return nil
}

func (insert *seriesInfo) prepareInsertRow() (times rawbinary, data rawbinary, count int, seqNum int32, minTime Timestamptz, maxTime Timestamptz) {
	// the series must already be sorted
	minTime = insert.pending.timestamps[0]
	maxTime = insert.pending.timestamps[len(insert.pending.timestamps)-1]
	seqNum = insert.lastSeq
	count = len(insert.pending.timestamps)
	timeCompressor := compression.DeltaDeltaCompressor{}
	for _, t := range insert.pending.timestamps {
		timeCompressor.Append(uint64(t.Time))
	}
	timeBuf := bytes.Buffer{}
	timeBuf.Grow(int(timeCompressor.CompressedLen()))
	_, err := timeCompressor.WriteTo(&timeBuf)
	if err != nil {
		//TODO
		panic(err)
	}
	times = rawbinary{timeBuf.Bytes()}

	dataCompressor := compression.GorillaCompressor{}
	for _, d := range insert.pending.dataPoints {
		dataCompressor.Append(math.Float64bits(d))
	}
	dataBuf := bytes.Buffer{}
	dataBuf.Grow(int(dataCompressor.CompressedLen()))
	_, err = dataCompressor.WriteTo(&dataBuf)
	if err != nil {
		//TODO
		panic(err)
	}
	data = rawbinary{dataBuf.Bytes()}
	return
}

func (insert *seriesInfo) finishedFragment() {
	notify := insert.pending.notify
	for _, waiter := range notify {
		waiter.finishedFragment()
	}
}

func (waiter *finishedNotify) finishedFragment() {
	waiter.numFragments -= 1
	if waiter.numFragments == 0 {
		waiter.onFinished.Done()
	}
}

func (h *insertHandler) setSeriesIds(req *insertDataRequest) (string, error) {
	numMissingSeries := 0

	for i, series := range req.data {
		id, ok := h.seriesCache[series.labels.String()]
		if ok {
			req.data[i].seriesID = id
		} else {
			numMissingSeries++
		}
	}

	if numMissingSeries == 0 {
		return "", nil
	}

	seriesToInsert := make([]*samplesInfo, 0, numMissingSeries)
	for i, series := range req.data {
		if series.seriesID < 0 {
			seriesToInsert = append(seriesToInsert, &req.data[i])
		}
	}
	var lastSeenLabel Labels

	batch := h.conn.NewBatch()
	numSQLFunctionCalls := 0
	// Sort and remove duplicates. The sort is needed to remove duplicates. Each series is inserted
	// in a different transaction, thus deadlocks are not an issue.
	sort.Slice(seriesToInsert, func(i, j int) bool {
		return seriesToInsert[i].labels.Compare(seriesToInsert[j].labels) < 0
	})

	batchSeries := make([][]*samplesInfo, 0, len(seriesToInsert))
	// group the seriesToInsert by labels, one slice array per unique labels
	for _, curr := range seriesToInsert {
		if !lastSeenLabel.isEmpty() && lastSeenLabel.Equal(curr.labels) {
			batchSeries[len(batchSeries)-1] = append(batchSeries[len(batchSeries)-1], curr)
			continue
		}

		batch.Queue("BEGIN;")
		batch.Queue(getSeriesIDForLabelSQL, curr.labels.metricName, curr.labels.names, curr.labels.values)
		batch.Queue("COMMIT;")
		numSQLFunctionCalls++
		batchSeries = append(batchSeries, []*samplesInfo{curr})

		lastSeenLabel = curr.labels
	}

	br, err := h.conn.SendBatch(context.Background(), batch)
	if err != nil {
		return "", err
	}
	defer br.Close()

	if numSQLFunctionCalls != len(batchSeries) {
		return "", fmt.Errorf("unexpected difference in numQueries and batchSeries")
	}

	var tableName string
	for i := 0; i < numSQLFunctionCalls; i++ {
		_, err = br.Exec()
		if err != nil {
			return "", err
		}
		row := br.QueryRow()

		var id SeriesID
		err = row.Scan(&tableName, &id)
		if err != nil {
			return "", err
		}
		h.seriesCache[batchSeries[i][0].labels.String()] = id
		for _, lsi := range batchSeries[i] {
			lsi.seriesID = id
		}
		_, err = br.Exec()
		if err != nil {
			return "", err
		}
	}

	return tableName, nil
}

func (c *chunkInfo) Less(b btree.Item) bool {
	switch v := b.(type) {
	case *chunkInfo:
		return c.startTime.Time < v.startTime.Time
	case Timestamptz:
		return c.endTime.Time <= v.Time
	default:
		panic("unknown type")
	}
}

func (t Timestamptz) Less(b btree.Item) bool {
	switch v := b.(type) {
	case *chunkInfo:
		return t.Time < v.startTime.Time
	case Timestamptz:
		return t.Time < v.Time
	default:
		panic(fmt.Sprintf("unknown type %v", v))
	}
}

// NewPgxReaderWithMetricCache returns a new DBReader that reads from PostgreSQL using PGX
// and caches metric table names using the supplied cacher.
func NewPgxReaderWithMetricCache(c *pgxpool.Pool, cache MetricCache) *DBReader {
	pi := &pgxQuerier{
		conn: &pgxConnImpl{
			conn: c,
		},
		metricTableNames: cache,
	}

	return &DBReader{
		db: pi,
	}
}

// NewPgxReader returns a new DBReader that reads that from PostgreSQL using PGX.
func NewPgxReader(c *pgxpool.Pool) *DBReader {
	metrics, _ := bigcache.NewBigCache(DefaultCacheConfig())
	cache := &MetricNameCache{metrics}
	return NewPgxReaderWithMetricCache(c, cache)
}

type metricTimeRangeFilter struct {
	metric    string
	startTime string
	endTime   string
}

type pgxQuerier struct {
	conn             pgxConn
	metricTableNames MetricCache
}

// HealthCheck implements the healtchecker interface
func (q *pgxQuerier) HealthCheck() error {
	rows, err := q.conn.Query(context.Background(), "SELECT")

	if err != nil {
		return err
	}

	rows.Close()
	return nil
}

func (q *pgxQuerier) Query(query *prompb.Query) ([]*prompb.TimeSeries, error) {
	if query == nil {
		return []*prompb.TimeSeries{}, nil
	}

	metric, cases, values, err := buildSubQueries(query)
	if err != nil {
		return nil, err
	}
	filter := metricTimeRangeFilter{
		metric:    metric,
		startTime: toRFC3339Nano(query.StartTimestampMs),
		endTime:   toRFC3339Nano(query.EndTimestampMs),
	}

	if metric != "" {
		return q.querySingleMetric(metric, filter, cases, values)
	}

	sqlQuery := buildMetricNameSeriesIDQuery(cases)
	rows, err := q.conn.Query(context.Background(), sqlQuery, values...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	metrics, series, err := getSeriesPerMetric(rows)

	if err != nil {
		return nil, err
	}

	results := make([]*prompb.TimeSeries, 0, len(metrics))

	for i, metric := range metrics {
		tableName, err := q.getMetricTableName(metric)
		if err != nil {
			// If the metric table is missing, there are no results for this query.
			if err == errMissingTableName {
				continue
			}

			return nil, err
		}
		filter.metric = tableName
		sqlQuery = buildTimeseriesBySeriesIDQuery(filter, series[i])
		rows, err = q.conn.Query(context.Background(), sqlQuery)

		if err != nil {
			return nil, err
		}

		ts, err := buildTimeSeries(rows)
		rows.Close()

		if err != nil {
			return nil, err
		}

		results = append(results, ts...)
	}

	return results, nil
}

func (q *pgxQuerier) querySingleMetric(metric string, filter metricTimeRangeFilter, cases []string, values []interface{}) ([]*prompb.TimeSeries, error) {
	tableName, err := q.getMetricTableName(metric)
	if err != nil {
		// If the metric table is missing, there are no results for this query.
		if err == errMissingTableName {
			return make([]*prompb.TimeSeries, 0), nil
		}

		return nil, err
	}
	filter.metric = tableName

	sqlQuery := buildTimeseriesByLabelClausesQuery(filter, cases)
	rows, err := q.conn.Query(context.Background(), sqlQuery, values...)

	if err != nil {
		// If we are getting undefined table error, it means the query
		// is looking for a metric which doesn't exist in the system.
		if e, ok := err.(*pgconn.PgError); !ok || e.Code != pgerrcode.UndefinedTable {
			return nil, err
		}
	}

	defer rows.Close()
	return buildTimeSeries(rows)
}

func (q *pgxQuerier) getMetricTableName(metric string) (string, error) {
	var err error
	var tableName string

	tableName, err = q.metricTableNames.Get(metric)

	if err == nil {
		return tableName, nil
	}

	if err != ErrEntryNotFound {
		return "", err
	}

	tableName, err = q.queryMetricTableName(metric)

	if err != nil {
		return "", err
	}

	err = q.metricTableNames.Set(metric, tableName)

	return tableName, err
}

func (q *pgxQuerier) queryMetricTableName(metric string) (string, error) {
	res, err := q.conn.Query(
		context.Background(),
		getMetricsTableSQL,
		metric,
	)

	if err != nil {
		return "", err
	}

	var tableName string
	defer res.Close()
	if !res.Next() {
		return "", errMissingTableName
	}

	if err := res.Scan(&tableName); err != nil {
		return "", err
	}

	return tableName, nil
}
