package end_to_end_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgio"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/common/model"

	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/timescale/timescale-prometheus/pkg/pgmodel"
)

func TestTimeConvert(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	withDB(t, *testDatabase, func(db *pgxpool.Pool, t testing.TB) {
		cases := []struct {
			input string
		}{
			{
				input: "0800-11-20 23:51:01+00",
			},
			{
				input: "1970-01-01 00:00:00+00",
			},
			{
				input: "2000-01-01 00:00:00+00",
			},
			{
				input: "1840-07-13 09:21:53+00",
			},
			{
				input: "2020-05-19 16:02:11+00",
			},
			{
				input: "2800-02-28 03:33:33+00",
			},
		}
		conn, err := db.Acquire(context.Background())
		defer conn.Release()
		if err != nil {
			t.Fatalf("%v\n", err)
		}
		rows, err := conn.Query(context.Background(), "SET timezone TO 'UTC'")
		if err != nil {
			t.Fatalf("%v\n", err)
		}
		rows.Close()
		for _, c := range cases {
			time.Local = time.UTC
			const pgTimeFormat = "2006-01-02 15:04:05Z07"
			goTime, err := time.ParseInLocation(pgTimeFormat, c.input, time.UTC)
			if err != nil {
				t.Fatalf("%v\n", err)
			}

			//validate that the time is representable
			var ouputtedTime string
			err = conn.QueryRow(context.Background(), fmt.Sprintf("SELECT '%s'::TIMESTAMPTZ::TEXT", c.input)).Scan(&ouputtedTime)
			if err != nil {
				t.Fatalf("%v\n", err)
			}
			if ouputtedTime != c.input {
				t.Errorf("wrong time val\nexpected\n%v\ngot\n%v\n", c.input, ouputtedTime)
			}

			goOut, err := time.ParseInLocation(pgTimeFormat, ouputtedTime, time.UTC)
			if err != nil {
				t.Fatalf("%v\n", err)
			}
			if goOut != goTime {
				t.Errorf("wrong time val\nexpected\n%v\ngot\n%v\n", goOut, goTime)
			}

			const msPerS = 1000
			const nsPerS = 1e9
			const nsPerMs = 1000000
			promTime := model.TimeFromUnix(goTime.Unix())
			conversionError := goTime.Sub(promTime.Time())
			if int64(conversionError) < 0 {
				conversionError = time.Duration(-int64(conversionError))
			}
			if int64(conversionError) > 0 {
				t.Logf("conversion error for %s: %v", c.input, conversionError)
			}
			pgTime, isInf := pgmodel.ToPostgresTime(promTime)
			if isInf {
				t.Logf("time %s out of range to convert to postgres", c.input)
			}
			// validate that inputting the raw time gets us the timestamp we expect
			err = conn.QueryRow(context.Background(), "SELECT $1::TIMESTAMPTZ::TEXT", pgTime).Scan(&ouputtedTime)
			if err != nil {
				t.Fatalf("%v\n", err)
			}
			if ouputtedTime != c.input {
				t.Errorf("wrong time val\nexpected\n%v\ngot\n%v\n", c.input, ouputtedTime)
			}

			// validate that the raw time we get back is the one we expect
			rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT '%s'::TIMESTAMPTZ", c.input))
			if err != nil {
				t.Fatalf("%v\n", err)
			}
			if !rows.Next() {
				t.Error("no rows")
			}
			var rawTimestamp pgmodel.Timestamptz
			rows.Scan(&rawTimestamp)
			rows.Close()
			if err != nil {
				t.Fatalf("%v\n", err)
			}

			if rawTimestamp != pgTime {
				t.Errorf("wrong pg time val for: %s\nexpected\n%v\ngot\n%v\n", c.input, pgTime, rawTimestamp)
			}

			// validate FromPostgresTime
			convertedBack, isInf := pgmodel.FromPostgresTime(rawTimestamp)
			if isInf {
				t.Logf("time %s is cannot be converted from pg time to prom time", c.input)
			}
			if promTime != convertedBack {
				t.Errorf("wrong prom time val for: %s\nexpected\n%v\ngot\n%v\n", c.input, promTime, convertedBack)
			}
		}
	})
}

type binaryTimestamptz struct {
	timestamp int64
}

func (src binaryTimestamptz) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	return pgio.AppendInt64(buf, src.timestamp), nil
}
