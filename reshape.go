package main

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Job rewrites one measurement.
type Job struct {
	C               *Client
	Src             string
	Rules           *Rules
	Window          time.Duration
	Batch           int
	StatePath       string
	BackupPath      string
	AllowCollisions bool
	Log             io.Writer
}

// State is saved after every window, so an interrupted apply continues
// where it stopped.
type State struct {
	Fingerprint string `json:"fingerprint"`
	Step        int    `json:"step"`
	Next        int64  `json:"next"`
	Min         int64  `json:"min"`
	Max         int64  `json:"max"`
}

type step struct {
	name string
	kind string // backup, copy, drop
	src  string
	dst  string
	rule *Rules
}

func (j *Job) logf(format string, a ...any) {
	fmt.Fprintf(j.Log, format+"\n", a...)
}

func (j *Job) target() string {
	if j.Rules.RenameMeasurement != "" {
		return j.Rules.RenameMeasurement
	}
	return j.Src
}

func (j *Job) inPlace() bool { return j.target() == j.Src }

func (j *Job) stage() string { return j.Src + "__reshape_tmp" }

func (j *Job) steps() []step {
	var s []step
	if j.BackupPath != "" {
		s = append(s, step{name: fmt.Sprintf("back up %q to %s", j.Src, j.BackupPath), kind: "backup", src: j.Src})
	}
	if j.inPlace() {
		s = append(s,
			step{name: fmt.Sprintf("copy %q -> %q with the changes, verify counts", j.Src, j.stage()), kind: "copy", src: j.Src, dst: j.stage(), rule: j.Rules},
			step{name: fmt.Sprintf("drop measurement %q", j.Src), kind: "drop", src: j.Src},
			step{name: fmt.Sprintf("copy %q -> %q, verify counts", j.stage(), j.Src), kind: "copy", src: j.stage(), dst: j.Src, rule: NewRules()},
			step{name: fmt.Sprintf("drop measurement %q", j.stage()), kind: "drop", src: j.stage()},
		)
	} else {
		s = append(s,
			step{name: fmt.Sprintf("copy %q -> %q with the changes, verify counts", j.Src, j.target()), kind: "copy", src: j.Src, dst: j.target(), rule: j.Rules},
			step{name: fmt.Sprintf("drop measurement %q", j.Src), kind: "drop", src: j.Src},
		)
	}
	return s
}

func (j *Job) fingerprint() string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%d\x00%s\n", j.C.URL, j.C.DB, j.C.RP, j.Src, j.Window, j.BackupPath)
	for _, l := range j.Rules.Describe(j.Src) {
		fmt.Fprintln(h, l)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// ResolveRP makes the retention policy explicit on 1.x, because DROP
// MEASUREMENT and the safety checks depend on it.
func (j *Job) ResolveRP(ctx context.Context) error {
	if j.C.Major != 1 {
		return nil
	}
	rps, def, err := j.retentionPolicies(ctx)
	if err != nil {
		return err
	}
	if j.C.RP == "" {
		j.C.RP = def
		return nil
	}
	for _, rp := range rps {
		if rp == j.C.RP {
			return nil
		}
	}
	return fmt.Errorf("database %q has no retention policy %q", j.C.DB, j.C.RP)
}

func (j *Job) retentionPolicies(ctx context.Context) (all []string, def string, err error) {
	err = j.C.Query(ctx, "SHOW RETENTION POLICIES ON "+ident(j.C.DB), func(_ int, s Series) error {
		for _, row := range s.Values {
			name, _ := row[0].(string)
			all = append(all, name)
			if d, _ := row[len(row)-1].(bool); d {
				def = name
			}
		}
		return nil
	})
	return all, def, err
}

// Schema reads field keys (with all types) and tag keys.
func (j *Job) Schema(ctx context.Context, m string) (*Schema, error) {
	s := &Schema{FieldTypes: map[string][]string{}}
	err := j.C.Query(ctx, "SHOW FIELD KEYS FROM "+j.C.from(m), func(_ int, se Series) error {
		for _, row := range se.Values {
			k, _ := row[0].(string)
			t, _ := row[1].(string)
			if _, ok := s.FieldTypes[k]; !ok {
				s.Fields = append(s.Fields, k)
			}
			s.FieldTypes[k] = append(s.FieldTypes[k], t)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = j.C.Query(ctx, "SHOW TAG KEYS FROM "+j.C.from(m), func(_ int, se Series) error {
		for _, row := range se.Values {
			k, _ := row[0].(string)
			s.Tags = append(s.Tags, k)
		}
		return nil
	})
	return s, err
}

// TimeRange returns the first and last timestamp of a measurement.
// It uses the explicit per-type select list: SELECT * misses points whose
// field has a different type than the one SELECT * picks.
func (j *Job) TimeRange(ctx context.Context, m string) (min, max int64, ok bool, err error) {
	s, err := j.Schema(ctx, m)
	if err != nil {
		return 0, 0, false, err
	}
	sel, _ := selectList(s, "")
	if sel == "" {
		return 0, 0, false, nil
	}
	read := func(order string) (int64, bool, error) {
		var t int64
		found := false
		err := j.C.Query(ctx, "SELECT "+sel+" FROM "+j.C.from(m)+" ORDER BY time "+order+" LIMIT 1", func(_ int, s Series) error {
			if len(s.Values) > 0 {
				n, _ := s.Values[0][0].(json.Number)
				v, err := strconv.ParseInt(n.String(), 10, 64)
				if err != nil {
					return err
				}
				t, found = v, true
			}
			return nil
		})
		return t, found, err
	}
	if min, ok, err = read("ASC"); err != nil || !ok {
		return 0, 0, ok, err
	}
	max, _, err = read("DESC")
	return min, max, true, err
}

// column says which field and type an aliased result column holds.
type column struct {
	field string
	typ   string
}

// selectList builds an explicit, aliased select list with one column per
// field type. SELECT * would return only one type of a field whose type
// differs between shards and silently skip the other values.
func selectList(s *Schema, agg string) (string, map[string]column) {
	var parts []string
	cols := map[string]column{}
	for _, f := range s.Fields {
		for _, t := range s.FieldTypes[f] {
			alias := fmt.Sprintf("__ir%d", len(cols))
			expr := ident(f) + "::" + t
			if agg != "" {
				expr = agg + "(" + expr + ")"
			}
			parts = append(parts, expr+" AS "+alias)
			cols[alias] = column{f, t}
		}
	}
	return strings.Join(parts, ", "), cols
}

func timeCond(a, b int64) string {
	return fmt.Sprintf("time >= %d AND time < %d", a, b)
}

// readWindow streams the points of [a, b) as they are stored. With
// numericAsFloat, values of a field with a numeric type conflict are read
// as floats (see pickValue).
func (j *Job) readWindow(ctx context.Context, m string, s *Schema, a, b int64, numericAsFloat bool, fn func(*Point) error) error {
	sel, cols := selectList(s, "")
	if sel == "" {
		return nil
	}
	q := "SELECT " + sel + " FROM " + j.C.from(m) + " WHERE " + timeCond(a, b) + " GROUP BY *"
	return j.C.Query(ctx, q, func(_ int, se Series) error {
		idx := make([]column, len(se.Columns))
		for i, c := range se.Columns {
			idx[i] = cols[c]
		}
		for _, row := range se.Values {
			n, _ := row[0].(json.Number)
			p := &Point{Measurement: m, Tags: se.Tags, Time: n.String()}
			vals := map[string]map[string]any{}
			var order []string
			for i := 1; i < len(row); i++ {
				if row[i] == nil || idx[i].field == "" {
					continue
				}
				f := idx[i].field
				if vals[f] == nil {
					vals[f] = map[string]any{}
					order = append(order, f)
				}
				vals[f][idx[i].typ] = row[i]
			}
			for _, f := range order {
				v, err := pickValue(vals[f], s.FieldTypes[f], numericAsFloat)
				if err != nil {
					return fmt.Errorf("field %q at %s: %w", f, p.Time, err)
				}
				p.Fields = append(p.Fields, FieldValue{Key: f, Value: v})
			}
			if len(p.Fields) == 0 {
				continue
			}
			if err := fn(p); err != nil {
				return err
			}
		}
		return nil
	})
}

// pickValue recovers the stored value of a field from its per-type columns.
// InfluxQL casts between float, integer and unsigned, so for a numeric
// value all numeric columns are filled and a whole float cannot be told
// apart from an integer; string and boolean are exact. A whole number is
// taken from the integer column (exact above 2^53), unless asFloat is set:
// the backup uses floats so that a restore does not mix types in a shard.
func pickValue(byType map[string]any, types []string, asFloat bool) (Value, error) {
	if len(types) == 1 {
		return decodeValue(byType[types[0]], types[0])
	}
	for _, t := range []string{tString, tBoolean} {
		if v, ok := byType[t]; ok {
			return decodeValue(v, t)
		}
	}
	fv, hasF := byType[tFloat]
	if hasF {
		v, err := decodeValue(fv, tFloat)
		if err != nil {
			return v, err
		}
		f, _ := strconv.ParseFloat(v.Num, 64)
		if asFloat || f != math.Trunc(f) {
			return v, nil
		}
	}
	for _, t := range []string{tInteger, tUnsigned} {
		if v, ok := byType[t]; ok {
			return decodeValue(v, t)
		}
	}
	if hasF {
		return decodeValue(fv, tFloat)
	}
	return Value{}, errors.New("no value")
}

// Count returns the number of values per field in [a, b).
func (j *Job) Count(ctx context.Context, m string, a, b int64) (map[string]int64, error) {
	out := map[string]int64{}
	err := j.C.Query(ctx, "SELECT COUNT(*) FROM "+j.C.from(m)+" WHERE "+timeCond(a, b), func(_ int, s Series) error {
		for _, row := range s.Values {
			for i := 1; i < len(row) && i < len(s.Columns); i++ {
				n, ok := row[i].(json.Number)
				if !ok {
					continue
				}
				c, err := strconv.ParseInt(n.String(), 10, 64)
				if err != nil {
					return err
				}
				out[strings.TrimPrefix(s.Columns[i], "count_")] += c
			}
		}
		return nil
	})
	return out, err
}

// CountByType counts values per field and type over the whole measurement.
// Unlike COUNT(*), it also sees values of the other types of a field.
func (j *Job) CountByType(ctx context.Context, m string, s *Schema) (map[string]int64, error) {
	sel, cols := selectList(s, "COUNT")
	out := map[string]int64{}
	if sel == "" {
		return out, nil
	}
	err := j.C.Query(ctx, "SELECT "+sel+" FROM "+j.C.from(m), func(_ int, se Series) error {
		for _, row := range se.Values {
			for i := 1; i < len(row); i++ {
				if n, ok := row[i].(json.Number); ok {
					c, _ := strconv.ParseInt(n.String(), 10, 64)
					col := cols[se.Columns[i]]
					out[col.field+"\x00"+col.typ] += c
				}
			}
		}
		return nil
	})
	return out, err
}

func windows(min, max int64, w time.Duration) [][2]int64 {
	step := int64(w)
	start := min - mod(min, step)
	var out [][2]int64
	for a := start; a <= max; a += step {
		out = append(out, [2]int64{a, a + step})
		if a > math.MaxInt64-step {
			break
		}
	}
	return out
}

func mod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}

// CopyStats summarizes a copy.
type CopyStats struct {
	Points     int64
	Written    int64
	Collisions int64
	Values     map[string]int64
}

// copyWindow reads [a, b) from src, applies the rules and writes to dst
// (or only checks when dst is empty). It returns the expected per-field
// counts in dst.
func (j *Job) copyWindow(ctx context.Context, st step, s *Schema, a, b int64, write bool, stats *CopyStats) (map[string]int64, error) {
	expected := map[string]int64{}
	var seen map[string]map[string]bool
	if st.rule.MergesSeries() {
		seen = map[string]map[string]bool{}
	}
	dstName := st.dst
	if dstName == "" {
		dstName = j.target()
	}
	var buf []byte
	lines := 0
	flush := func() error {
		if lines == 0 {
			return nil
		}
		if write {
			if err := j.C.Write(ctx, buf); err != nil {
				return err
			}
		}
		stats.Written += int64(lines)
		buf, lines = buf[:0], 0
		return nil
	}
	err := j.readWindow(ctx, st.src, s, a, b, false, func(p *Point) error {
		stats.Points++
		keep, err := st.rule.Apply(p, dstName)
		if err != nil {
			return err
		}
		if !keep {
			return nil
		}
		if seen != nil {
			key := seriesKey(p) + "\x00" + p.Time
			fs := seen[key]
			if fs == nil {
				fs = map[string]bool{}
				seen[key] = fs
			}
			for _, f := range p.Fields {
				if fs[f.Key] {
					stats.Collisions++
					if !j.AllowCollisions {
						return fmt.Errorf("collision: two source points become series %s at time %s with field %q; "+
							"InfluxDB would keep only the last one (use --allow-collisions to accept that)",
							strings.ReplaceAll(seriesKey(p), "\x00", ","), p.Time, f.Key)
					}
					continue
				}
				fs[f.Key] = true
				expected[f.Key]++
			}
		} else {
			for _, f := range p.Fields {
				expected[f.Key]++
			}
		}
		buf = AppendLine(buf, p)
		lines++
		if lines >= j.Batch {
			return flush()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	for k, v := range expected {
		if stats.Values == nil {
			stats.Values = map[string]int64{}
		}
		stats.Values[k] += v
	}
	return expected, nil
}

func sameCounts(want, got map[string]int64) error {
	var diff []string
	for _, k := range sortedKeys(want) {
		if got[k] != want[k] {
			diff = append(diff, fmt.Sprintf("%s: want %d, got %d", k, want[k], got[k]))
		}
	}
	for _, k := range sortedKeys(got) {
		if _, ok := want[k]; !ok && got[k] != 0 {
			diff = append(diff, fmt.Sprintf("%s: want 0, got %d", k, got[k]))
		}
	}
	if len(diff) > 0 {
		return errors.New(strings.Join(diff, "; "))
	}
	return nil
}

func (j *Job) loadState() (*State, error) {
	b, err := os.ReadFile(j.StatePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, fmt.Errorf("%s: %w", j.StatePath, err)
	}
	return &st, nil
}

func (j *Job) saveState(st *State) error {
	b, _ := json.MarshalIndent(st, "", "  ")
	tmp := j.StatePath + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, j.StatePath)
}

// Preflight checks everything that can be checked before data is touched.
func (j *Job) Preflight(ctx context.Context, s *Schema) error {
	if err := j.Rules.Check(s); err != nil {
		return err
	}
	dst := j.stage()
	if !j.inPlace() {
		dst = j.target()
	}
	if _, _, ok, err := j.TimeRange(ctx, dst); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("measurement %q already has data; it must be empty", dst)
	}
	if j.C.Major == 1 {
		// DROP MEASUREMENT removes the measurement from every retention
		// policy of the database, not only from the one being rewritten.
		rps, _, err := j.retentionPolicies(ctx)
		if err != nil {
			return err
		}
		for _, rp := range rps {
			if rp == j.C.RP {
				continue
			}
			other := *j.C
			other.RP = rp
			oj := *j
			oj.C = &other
			for _, m := range []string{j.Src, dst} {
				if _, _, ok, err := oj.TimeRange(ctx, m); err != nil {
					return err
				} else if ok {
					return fmt.Errorf("measurement %q also has data in retention policy %q; "+
						"DROP MEASUREMENT would delete it there too", m, rp)
				}
			}
		}
	}
	return nil
}

// Apply runs (or resumes) all steps.
func (j *Job) Apply(ctx context.Context) error {
	st, err := j.loadState()
	if err != nil {
		return err
	}
	fp := j.fingerprint()
	if st != nil && st.Fingerprint != fp {
		return fmt.Errorf("%s belongs to a different job; remove it to start over", j.StatePath)
	}
	if st == nil {
		s, err := j.Schema(ctx, j.Src)
		if err != nil {
			return err
		}
		if len(s.Fields) == 0 {
			return fmt.Errorf("measurement %q has no fields (does it exist in %s?)", j.Src, j.C.from(j.Src))
		}
		if err := j.Preflight(ctx, s); err != nil {
			return err
		}
		min, max, ok, err := j.TimeRange(ctx, j.Src)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("measurement %q has no data", j.Src)
		}
		st = &State{Fingerprint: fp, Min: min, Max: max, Next: math.MinInt64}
		if err := j.saveState(st); err != nil {
			return err
		}
	} else {
		j.logf("resuming at step %d from %s", st.Step+1, j.StatePath)
	}
	steps := j.steps()
	for ; st.Step < len(steps); st.Step, st.Next = st.Step+1, math.MinInt64 {
		if err := j.saveState(st); err != nil {
			return err
		}
		sp := steps[st.Step]
		j.logf("step %d/%d: %s", st.Step+1, len(steps), sp.name)
		switch sp.kind {
		case "backup":
			if err := j.backup(ctx, st); err != nil {
				return err
			}
		case "drop":
			if err := j.C.DropMeasurement(ctx, sp.src); err != nil {
				return err
			}
			if _, _, ok, err := j.TimeRange(ctx, sp.src); err != nil {
				return err
			} else if ok {
				return fmt.Errorf("measurement %q still has data after the drop; is something still writing to it?", sp.src)
			}
		case "copy":
			if err := j.copyStep(ctx, sp, st); err != nil {
				return err
			}
		}
	}
	if err := os.Remove(j.StatePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	j.logf("done")
	return nil
}

func (j *Job) copyStep(ctx context.Context, sp step, st *State) error {
	s, err := j.Schema(ctx, sp.src)
	if err != nil {
		return err
	}
	var stats CopyStats
	for _, w := range windows(st.Min, st.Max, j.Window) {
		if w[1] <= st.Next {
			continue
		}
		want, err := j.copyWindow(ctx, sp, s, w[0], w[1], true, &stats)
		if err != nil {
			return fmt.Errorf("window %s: %w", fmtWindow(w), err)
		}
		got, err := j.Count(ctx, sp.dst, w[0], w[1])
		if err != nil {
			return err
		}
		if err := sameCounts(want, got); err != nil {
			return fmt.Errorf("verification of %q failed in window %s: %v", sp.dst, fmtWindow(w), err)
		}
		st.Next = w[1]
		if err := j.saveState(st); err != nil {
			return err
		}
	}
	j.logf("  %d points read, %d written, %d values verified%s", stats.Points, stats.Written, sum(stats.Values), collisionNote(stats.Collisions))
	return nil
}

func collisionNote(n int64) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf(", %d colliding values merged", n)
}

func sum(m map[string]int64) int64 {
	var t int64
	for _, v := range m {
		t += v
	}
	return t
}

func fmtWindow(w [2]int64) string {
	return time.Unix(0, w[0]).UTC().Format(time.RFC3339) + ".." + time.Unix(0, w[1]).UTC().Format(time.RFC3339)
}

func (j *Job) backup(ctx context.Context, st *State) error {
	s, err := j.Schema(ctx, j.Src)
	if err != nil {
		return err
	}
	f, err := os.Create(j.BackupPath)
	if err != nil {
		return err
	}
	defer f.Close()
	zw := gzip.NewWriter(f)
	fmt.Fprintf(zw, "# DML\n# CONTEXT-DATABASE: %s\n# CONTEXT-RETENTION-POLICY: %s\n", j.C.DB, j.C.RP)
	var n int64
	var buf []byte
	for _, w := range windows(st.Min, st.Max, j.Window) {
		err := j.readWindow(ctx, j.Src, s, w[0], w[1], true, func(p *Point) error {
			buf = AppendLine(buf[:0], p)
			n++
			_, err := zw.Write(buf)
			return err
		})
		if err != nil {
			return err
		}
	}
	if err := zw.Close(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	j.logf("  %d points written to %s", n, j.BackupPath)
	return nil
}

// Plan prints what apply would do; with scan it also runs the rewrite over
// all data without writing, to find collisions and failing casts.
func (j *Job) Plan(ctx context.Context, w io.Writer, scan bool) error {
	s, err := j.Schema(ctx, j.Src)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "InfluxDB %s, database %q, retention policy %q\n", j.C.Version, j.C.DB, j.C.RP)
	if len(s.Fields) == 0 {
		return fmt.Errorf("measurement %q has no fields", j.Src)
	}
	min, max, ok, err := j.TimeRange(ctx, j.Src)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("measurement %q has no data", j.Src)
	}
	fmt.Fprintf(w, "measurement %q: %s .. %s\n", j.Src, time.Unix(0, min).UTC().Format(time.RFC3339Nano), time.Unix(0, max).UTC().Format(time.RFC3339Nano))
	counts, err := j.CountByType(ctx, j.Src, s)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "  tags: %s\n", strings.Join(quoteAll(s.Tags), ", "))
	for _, f := range s.Fields {
		// InfluxQL casts between the numeric types, so their values can
		// only be counted together; string and boolean are exact.
		var parts, numeric []string
		var numCount int64
		for _, t := range s.FieldTypes[f] {
			if t == tFloat || t == tInteger || t == tUnsigned {
				numeric = append(numeric, t)
				numCount = counts[f+"\x00"+t]
				continue
			}
			parts = append(parts, fmt.Sprintf("%s (%d)", t, counts[f+"\x00"+t]))
		}
		if len(numeric) > 0 {
			parts = append([]string{fmt.Sprintf("%s (%d)", strings.Join(numeric, "/"), numCount)}, parts...)
		}
		note := ""
		if len(s.FieldTypes[f]) > 1 {
			note = "  <- type conflict; SELECT * returns only one of these types"
		}
		fmt.Fprintf(w, "  field %q: %s%s\n", f, strings.Join(parts, ", "), note)
	}
	fmt.Fprintln(w, "changes:")
	if j.Rules.Empty() {
		fmt.Fprintln(w, "  (none)")
	}
	for _, l := range j.Rules.Describe(j.Src) {
		fmt.Fprintln(w, "  "+l)
	}
	fmt.Fprintln(w, "steps:")
	for i, sp := range j.steps() {
		fmt.Fprintf(w, "  %d. %s\n", i+1, sp.name)
	}
	if err := j.Preflight(ctx, s); err != nil {
		fmt.Fprintln(w, "problems:")
		for _, l := range strings.Split(err.Error(), "\n") {
			fmt.Fprintln(w, "  "+l)
		}
		return errors.New("plan has problems")
	}
	if !scan {
		fmt.Fprintln(w, "checks: ok (run with --scan to also test every point against the changes)")
		return nil
	}
	sp := j.steps()[0]
	if sp.kind == "backup" {
		sp = j.steps()[1]
	}
	var stats CopyStats
	for _, win := range windows(min, max, j.Window) {
		if _, err := j.copyWindow(ctx, sp, s, win[0], win[1], false, &stats); err != nil {
			return fmt.Errorf("scan, window %s: %w", fmtWindow(win), err)
		}
	}
	out := j.Rules.OutputFields(s)
	fmt.Fprintf(w, "scan: %d points would become %d points%s\n", stats.Points, stats.Written, collisionNote(stats.Collisions))
	keys := sortedKeys(out)
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "  field %q: %s (%d)\n", k, out[k], stats.Values[k])
	}
	return nil
}

func quoteAll(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = strconv.Quote(s)
	}
	return out
}
