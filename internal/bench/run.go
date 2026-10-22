// Package bench runs isolated, durable relational workloads with explicit inputs.
package bench

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/pager"
	"github.com/vennictus/sceptre/internal/sql"
	"github.com/vennictus/sceptre/internal/table"
)

type Options struct {
	Rows       []int
	Payloads   []int
	Operations int
	Batch      int
	Seed       int64
	Directory  string
	Storage    string
	Revision   string
}

type Environment struct {
	GoVersion  string `json:"go_version"`
	OS         string `json:"os"`
	Arch       string `json:"arch"`
	CPU        string `json:"cpu"`
	CPUs       int    `json:"logical_cpus"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	Storage    string `json:"storage"`
	Revision   string `json:"revision"`
	PageSize   uint32 `json:"page_size"`
	Durability string `json:"durability"`
	// ClockResolutionNS is the smallest observed monotonic clock step. Latency
	// samples below it are recorded as 0; Windows clocks can step 0.5 ms or more.
	ClockResolutionNS int64 `json:"clock_resolution_ns"`
}

type Result struct {
	Workload                    string  `json:"workload"`
	Rows                        int     `json:"initial_rows"`
	Payload                     int     `json:"payload_bytes"`
	Operations                  int     `json:"operations"`
	Batch                       int     `json:"batch"`
	Seed                        int64   `json:"seed"`
	P50NS                       int64   `json:"p50_ns"`
	P95NS                       int64   `json:"p95_ns"`
	P99NS                       int64   `json:"p99_ns"`
	MeanNS                      float64 `json:"mean_ns"`
	AllocatedBytes              uint64  `json:"allocated_bytes"`
	Allocations                 uint64  `json:"allocations"`
	SampledPeakHeapBytes        uint64  `json:"sampled_peak_heap_bytes"`
	ProcessHighWaterRSSBytes    uint64  `json:"process_high_water_rss_bytes,omitempty"`
	PhysicalWriteBytes          uint64  `json:"physical_write_bytes"`
	WriteCalls                  uint64  `json:"write_calls"`
	SyncCalls                   uint64  `json:"sync_calls"`
	RowMutations                uint64  `json:"row_mutations"`
	ApplicationMutationBytes    uint64  `json:"application_mutation_bytes"`
	PhysicalBytesPerRowMutation float64 `json:"physical_bytes_per_row_mutation"`
	WriteAmplification          float64 `json:"write_amplification"`
	FileBytesBefore             int64   `json:"file_bytes_before"`
	FileBytesAfter              int64   `json:"file_bytes_after"`
	FinalRows                   int     `json:"final_rows"`
	AccessPath                  string  `json:"sql_access_path,omitempty"`
}

type Report struct {
	Version     int         `json:"report_version"`
	RecordedUTC string      `json:"recorded_utc"`
	Environment Environment `json:"environment"`
	Results     []Result    `json:"results"`
}

var workloads = []string{"point_read", "range_scan", "secondary_lookup", "sql_limited_scan", "single_insert", "batch_insert", "indexed_update", "indexed_delete_insert", "mixed_80_20"}

// Run creates only disposable directories beneath Directory (or the OS temp dir).
// progress receives a completed result after integrity and reopen verification.
func Run(opts Options, progress func(Result)) (Report, error) {
	if len(opts.Rows) == 0 || len(opts.Payloads) == 0 || opts.Operations < 1 || opts.Operations > 100000 || opts.Batch < 1 || opts.Batch > 1000 {
		return Report{}, fmt.Errorf("bench: require rows/payloads, 1..100000 operations, and 1..1000 batch size")
	}
	for _, rows := range opts.Rows {
		if rows < 1 || rows > 100000 {
			return Report{}, fmt.Errorf("bench: rows must be 1..100000")
		}
	}
	if duplicates(opts.Rows) || duplicates(opts.Payloads) {
		return Report{}, fmt.Errorf("bench: repeated dataset sizes are unsupported")
	}
	for _, size := range opts.Payloads {
		if size < 0 || size > 2048 {
			return Report{}, fmt.Errorf("bench: payload must be 0..2048 bytes")
		}
	}
	root, err := os.MkdirTemp(opts.Directory, "sceptre-bench-")
	if err != nil {
		return Report{}, err
	}
	defer os.RemoveAll(root)
	report := Report{Version: 1, RecordedUTC: time.Now().UTC().Format(time.RFC3339), Environment: Environment{
		GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, CPU: cpuName(), CPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0),
		Storage: opts.Storage, Revision: opts.Revision, PageSize: pager.DefaultPageSize, Durability: "page sync + metadata sync per commit; no relaxed durability",
		ClockResolutionNS: clockResolution(),
	}}
	for _, rows := range opts.Rows {
		for _, payload := range opts.Payloads {
			for _, workload := range workloads {
				result, err := runCase(root, opts, rows, payload, workload)
				if err != nil {
					return report, fmt.Errorf("bench: %s rows=%d payload=%d: %w", workload, rows, payload, err)
				}
				report.Results = append(report.Results, result)
				if progress != nil {
					progress(result)
				}
			}
		}
	}
	return report, nil
}

func runCase(root string, opts Options, rows, payload int, workload string) (Result, error) {
	path := filepath.Join(root, fmt.Sprintf("%s-%d-%d.db", workload, rows, payload))
	r := Result{Workload: workload, Rows: rows, Payload: payload, Operations: opts.Operations, Batch: opts.Batch, Seed: opts.Seed, FinalRows: rows}
	hooks := &pager.IOHooks{
		WriteAt: func(f *os.File, data []byte, offset int64) (int, error) {
			n, err := f.WriteAt(data, offset)
			r.WriteCalls++
			r.PhysicalWriteBytes += uint64(n)
			return n, err
		},
		Sync: func(f *os.File) error { r.SyncCalls++; return f.Sync() },
	}
	store, err := kv.Open(path, kv.Options{IO: hooks})
	if err != nil {
		return r, err
	}
	defer func() { store.Close() }()
	db := table.New(store)
	if err := db.CreateTable(table.TableDef{Name: "bench", Columns: []table.Column{{Name: "id", Type: table.TypeInt64}, {Name: "bucket", Type: table.TypeInt64}, {Name: "payload", Type: table.TypeBytes}}, PrimaryKey: []string{"id"}}); err != nil {
		return r, err
	}
	if err := db.CreateIndex("bench", table.IndexDef{Name: "bucket_idx", Columns: []string{"bucket"}}); err != nil {
		return r, err
	}
	body := []byte(strings.Repeat("x", payload))
	for start := 0; start < rows; start += 1000 {
		count := min(1000, rows-start)
		records := make([]table.Record, count)
		for i := range records {
			records[i] = record(start+i, (start+i)%64, body)
		}
		if err := db.InsertMany("bench", records); err != nil {
			return r, err
		}
	}
	if workload == "sql_limited_scan" {
		plan, err := sql.Explain(db, fmt.Sprintf("select id from bench where bucket >= 0 limit %d", opts.Batch))
		if err != nil {
			return r, err
		}
		r.AccessPath = string(plan.Access)
	}
	info, err := os.Stat(path)
	if err != nil {
		return r, err
	}
	r.FileBytesBefore = info.Size()
	r.WriteCalls, r.PhysicalWriteBytes, r.SyncCalls = 0, 0, 0
	rng := rand.New(rand.NewSource(opts.Seed))
	samples := make([]int64, opts.Operations)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	peak, stop := sampleHeap(before.HeapAlloc)
	for step := range samples {
		started := time.Now()
		err := operation(db, workload, step, rows, opts.Batch, payload, body, rng, &r)
		samples[step] = time.Since(started).Nanoseconds()
		if err != nil {
			stop()
			return r, fmt.Errorf("operation %d: %w", step, err)
		}
	}
	stop()
	runtime.ReadMemStats(&after)
	r.AllocatedBytes, r.Allocations = after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs
	r.SampledPeakHeapBytes = max(peak.Load(), after.HeapAlloc)
	r.ProcessHighWaterRSSBytes = highWaterRSS()
	r.P50NS, r.P95NS, r.P99NS = Quantile(samples, 0.50), Quantile(samples, 0.95), Quantile(samples, 0.99)
	for _, ns := range samples {
		r.MeanNS += float64(ns) / float64(len(samples))
	}
	if r.RowMutations != 0 {
		r.PhysicalBytesPerRowMutation = float64(r.PhysicalWriteBytes) / float64(r.RowMutations)
	}
	if r.ApplicationMutationBytes != 0 {
		r.WriteAmplification = float64(r.PhysicalWriteBytes) / float64(r.ApplicationMutationBytes)
	}
	info, err = os.Stat(path)
	if err != nil {
		return r, err
	}
	r.FileBytesAfter = info.Size()
	if err := verify(db, r.FinalRows); err != nil {
		return r, err
	}
	if err := store.Close(); err != nil {
		return r, err
	}
	store, err = kv.Open(path, kv.Options{})
	if err != nil {
		return r, err
	}
	if err := verify(table.New(store), r.FinalRows); err != nil {
		return r, err
	}
	return r, store.Close()
}

func operation(db *table.DB, workload string, step, rows, batch, payload int, body []byte, rng *rand.Rand, r *Result) error {
	id := rng.Intn(rows)
	key := func(id int) table.Record {
		return table.NewRecord(map[string]table.Value{"id": table.Int64Value(int64(id))})
	}
	switch workload {
	case "point_read":
		row, ok, err := db.Get("bench", key(id))
		if err != nil {
			return err
		}
		if !ok || row.Values["id"].I64 != int64(id) {
			return fmt.Errorf("point read mismatch")
		}
	case "range_scan":
		count := min(batch, rows-id)
		scan, err := db.Scan("bench", table.ScanBounds{Lower: table.Inclusive(key(id)), Upper: table.Exclusive(key(id + count))})
		if err != nil {
			return err
		}
		defer scan.Close()
		seen := 0
		for scan.Valid() {
			row, err := scan.Deref()
			if err != nil {
				return err
			}
			if row.Values["id"].I64 != int64(id+seen) {
				return fmt.Errorf("range mismatch")
			}
			seen++
			if err := scan.Next(); err != nil {
				return err
			}
		}
		if seen != count {
			return fmt.Errorf("range count %d != %d", seen, count)
		}
	case "secondary_lookup":
		bucket := id % 64
		found, err := db.LookupIndex("bench", "bucket_idx", table.NewRecord(map[string]table.Value{"bucket": table.Int64Value(int64(bucket))}))
		if err != nil {
			return err
		}
		want := rows / 64
		if bucket < rows%64 {
			want++
		}
		if len(found) != want {
			return fmt.Errorf("secondary count %d != %d", len(found), want)
		}
	case "sql_limited_scan":
		result, err := sql.Execute(db, fmt.Sprintf("select id from bench where bucket >= 0 limit %d", batch))
		if err != nil {
			return err
		}
		if len(result.Rows) != min(rows, batch) {
			return fmt.Errorf("SQL result count mismatch")
		}
	case "single_insert":
		if err := db.Insert("bench", record(rows+step, (rows+step)%64, body)); err != nil {
			return err
		}
		r.FinalRows++
		r.RowMutations++
		r.ApplicationMutationBytes += uint64(payload + 16)
	case "batch_insert":
		records := make([]table.Record, batch)
		for i := range records {
			k := rows + step*batch + i
			records[i] = record(k, k%64, body)
		}
		if err := db.InsertMany("bench", records); err != nil {
			return err
		}
		r.FinalRows += batch
		r.RowMutations += uint64(batch)
		r.ApplicationMutationBytes += uint64(batch * (payload + 16))
	case "indexed_update":
		if err := db.Update("bench", record(id, (step+id+1)%64, body)); err != nil {
			return err
		}
		r.RowMutations++
		r.ApplicationMutationBytes += uint64(payload + 16)
	case "indexed_delete_insert":
		removed, err := db.Delete("bench", key(id))
		if err != nil {
			return err
		}
		if !removed {
			return fmt.Errorf("missing deleted row")
		}
		if err := db.Insert("bench", record(id, id%64, body)); err != nil {
			return err
		}
		r.RowMutations += 2
		r.ApplicationMutationBytes += uint64(8 + payload + 16)
	case "mixed_80_20":
		if step%5 == 0 {
			if err := db.Update("bench", record(id, (step+id+1)%64, body)); err != nil {
				return err
			}
			r.RowMutations++
			r.ApplicationMutationBytes += uint64(payload + 16)
		} else {
			if _, ok, err := db.Get("bench", key(id)); err != nil {
				return err
			} else if !ok {
				return fmt.Errorf("mixed read missing")
			}
		}
	default:
		return fmt.Errorf("unknown workload %s", workload)
	}
	return nil
}

func record(id, bucket int, body []byte) table.Record {
	return table.NewRecord(map[string]table.Value{"id": table.Int64Value(int64(id)), "bucket": table.Int64Value(int64(bucket)), "payload": table.BytesValue(body)})
}

func verify(db *table.DB, rows int) error {
	report, err := db.Check()
	if err != nil {
		return err
	}
	if !report.OK() || len(report.Tables) != 1 || report.Tables[0].Rows != rows {
		return fmt.Errorf("integrity/row count failed: %+v", report)
	}
	return nil
}

// Quantile uses the nearest-rank definition. Input is copied before sorting.
func Quantile(samples []int64, q float64) int64 {
	if len(samples) == 0 {
		return 0
	}
	ordered := append([]int64(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := max(0, min(len(ordered)-1, int(math.Ceil(q*float64(len(ordered))))-1))
	return ordered[index]
}

func sampleHeap(initial uint64) (*atomic.Uint64, func()) {
	peak := &atomic.Uint64{}
	peak.Store(initial)
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-done:
				return
			case <-tick.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapAlloc > peak.Load() {
					peak.Store(m.HeapAlloc)
				}
			}
		}
	}()
	return peak, func() { close(done); <-stopped }
}

// clockResolution estimates the smallest nonzero step of the monotonic clock.
func clockResolution() int64 {
	best := int64(math.MaxInt64)
	for i := 0; i < 20; i++ {
		start := time.Now()
		var step time.Duration
		for step == 0 {
			step = time.Since(start)
		}
		if int64(step) < best {
			best = int64(step)
		}
	}
	return best
}

func cpuName() string {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "model name") {
				_, name, _ := strings.Cut(line, ":")
				return strings.TrimSpace(name)
			}
		}
	}
	if value := os.Getenv("PROCESSOR_IDENTIFIER"); value != "" {
		return value
	}
	return "unspecified"
}

func highWaterRSS() uint64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "VmHWM:" {
			value, _ := strconv.ParseUint(fields[1], 10, 64)
			return value * 1024
		}
	}
	return 0
}

func duplicates(values []int) bool {
	seen := make(map[int]bool)
	for _, value := range values {
		if seen[value] {
			return true
		}
		seen[value] = true
	}
	return false
}
