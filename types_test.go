package sceptre

import (
	"testing"
	"time"
)

func TestFloatBoolTimestampThroughThePublicAPI(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, "create table ev (id int64, score float64, done bool, at timestamp, primary key (id))")
	at := time.Date(2026, 11, 2, 10, 0, 0, 0, time.FixedZone("IST", 5*3600+1800))
	execOK(t, db, "insert into ev (id, score, done, at) values (?, ?, ?, ?)", Int64(1), Float64(2.5), Bool(true), Timestamp(at))
	res := execOK(t, db, "select score, done, at from ev where at = '2026-11-02T04:30:00Z'")
	if len(res.Rows) != 1 || res.Rows[0][0].F64 != 2.5 || !res.Rows[0][1].Bool || !res.Rows[0][2].Time().Equal(at) || res.Rows[0][2].Time().Location() != time.UTC {
		t.Fatalf("row = %+v", res.Rows)
	}
}
