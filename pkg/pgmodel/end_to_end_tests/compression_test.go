package end_to_end_tests

import (
	"bytes"
	"context"
	"encoding/base64"
	"math"
	"math/rand"
	"testing"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/timescale/timescale-prometheus/pkg/compression"
)

func TestCompressFloat(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	vals := make([]float64, 1010)
	for i := 0; i < 1000; i++ {
		vals[i] = float64(i)
	}
	vals[1000] = math.Inf(1)
	vals[1001] = math.Inf(-1)
	vals[1002] = math.NaN()
	for i := 1003; i < 1009; i++ {
		vals[i] = rand.NormFloat64()
	}
	rand.Shuffle(len(vals), func(i, j int) {
		vals[i], vals[j] = vals[j], vals[i]
	})

	compressor := compression.GorillaCompressor{}
	for _, val := range vals {
		compressor.Append(math.Float64bits(val))
	}

	buffer := bytes.Buffer{}
	_, err := compressor.WriteTo(&buffer)
	if err != nil {
		t.Fatalf("compression error: %v", err)
	}
	withSuperuser(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// FIXME we can't rely on the timescale version being 1.7.0...
		_, e := db.Exec(context.Background(),
			"CREATE OR REPLACE FUNCTION decompress_float64 (_timescaledb_internal.compressed_data, DOUBLE PRECISION) RETURNS TABLE (value DOUBLE PRECISION) AS '$libdir/timescaledb-1.7.0', 'ts_compressed_data_decompress_forward' ROWS 1000 LANGUAGE C IMMUTABLE PARALLEL SAFE")
		if e != nil {
			t.Fatal(e)
		}

		rows, err := db.Query(context.Background(), "SELECT decompress_float64($1::_timescaledb_internal.compressed_data, NULL::DOUBLE PRECISION)", rawbinary{buffer.Bytes()})
		if err != nil {
			t.Fatalf("decompression error: %v", err)
		}
		defer rows.Close()
		for i := 0; rows.Next(); i++ {
			var val float64
			err = rows.Scan(&val)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}
			if val != vals[i] && !(math.IsNaN(val) && math.IsNaN(vals[i])) {
				t.Errorf("wrong val[%d] %v != %v", i, val, vals[i])
			}
		}
	})
}

func TestCompressInt(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	vals := make([]uint64, 1010)
	for i := uint64(0); i < 1000; i++ {
		vals[i] = i
	}
	vals[1000] = math.MaxUint64
	vals[1001] = uint64(math.MaxInt64)
	for i := 1002; i < 1009; i++ {
		vals[i] = rand.Uint64()
	}
	rand.Shuffle(len(vals), func(i, j int) {
		vals[i], vals[j] = vals[j], vals[i]
	})

	compressor := compression.DeltaDeltaCompressor{}
	for _, val := range vals {
		compressor.Append(val)
	}

	buffer := bytes.Buffer{}
	_, err := compressor.WriteTo(&buffer)
	if err != nil {
		t.Fatalf("compression error: %v", err)
	}
	withSuperuser(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		// FIXME we can't rely on the timescale version being 1.7.0...
		_, e := db.Exec(context.Background(),
			"CREATE OR REPLACE FUNCTION decompress_uint64 (_timescaledb_internal.compressed_data, BIGINT) RETURNS TABLE (value BIGINT) AS '$libdir/timescaledb-1.7.0', 'ts_compressed_data_decompress_forward' ROWS 1000 LANGUAGE C IMMUTABLE PARALLEL SAFE")
		if e != nil {
			t.Fatal(e)
		}

		bb := buffer.Bytes()
		str := base64.StdEncoding.EncodeToString(bb)
		resStr := ""
		err := db.QueryRow(context.Background(), "SELECT $1::_timescaledb_internal.compressed_data::TEXT", rawbinary{bb}).Scan(&resStr)
		if err != nil {
			t.Fatalf("send error: %v", err)
		}
		if resStr != str {
			t.Errorf("incorrect send\n\ngot\n%s\nexpected\n%s\n", resStr, str)
		}

		rows, err := db.Query(context.Background(), "SELECT decompress_uint64($1::_timescaledb_internal.compressed_data, NULL::BIGINT)", rawbinary{bb})
		if err != nil {
			t.Fatalf("decompression error: %v", err)
		}
		rets := make([]uint64, 0, len(vals))
		for i := 0; rows.Next(); i++ {
			var val int64
			err = rows.Scan(&val)
			if err != nil {
				t.Fatalf("scan error: %v", err)
			}
			v := uint64(val)
			rets = append(rets, v)
			if v != vals[i] {
				t.Errorf("wrong val[%d] %v != %v", i, v, vals[i])
			}
		}
		rows.Close()
	})
}

type rawbinary struct {
	bytes []byte
}

func (src rawbinary) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	return append(buf, src.bytes...), nil
}
