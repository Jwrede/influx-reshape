package main

// End-to-end tests against a real InfluxDB. They run when IR_URL is set:
//   1.x: IR_URL=http://127.0.0.1:8086 IR_DB=test (database is created)
//   2.x: IR_URL=... IR_DB=<bucket> IR_TOKEN=... IR_ORG=...

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

type env1 struct {
	t    *testing.T
	c    *Client
	args []string
}

func setup(t *testing.T) *env1 {
	u := os.Getenv("IR_URL")
	if u == "" {
		t.Skip("IR_URL not set")
	}
	c := &Client{URL: u, DB: os.Getenv("IR_DB"), Token: os.Getenv("IR_TOKEN"), Org: os.Getenv("IR_ORG"), HTTP: http.DefaultClient}
	if c.DB == "" {
		c.DB = "irtest"
	}
	ctx := context.Background()
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if c.Major == 1 {
		if err := c.Query(ctx, "CREATE DATABASE "+ident(c.DB), func(int, Series) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"--url", u, "--db", c.DB}
	if c.Token != "" {
		args = append(args, "--token", c.Token, "--org", c.Org)
	}
	return &env1{t: t, c: c, args: args}
}

func (e *env1) write(lp string) {
	e.t.Helper()
	if err := e.c.Write(context.Background(), []byte(lp)); err != nil {
		e.t.Fatal(err)
	}
}

// name returns a measurement name unique to this test run.
func (e *env1) name(base string) string {
	return fmt.Sprintf("%s_%d", base, time.Now().UnixNano()%1e9)
}

func (e *env1) run(want int, args ...string) string {
	e.t.Helper()
	var out, errb bytes.Buffer
	full := append([]string{args[0]}, e.args...)
	full = append(full, args[1:]...)
	if args[0] == "apply" {
		full = append(full, "--state", filepath.Join(e.t.TempDir(), "state.json"))
	}
	code := run(full, &out, &errb)
	if code != want {
		e.t.Fatalf("%v: exit %d, want %d\nstdout:\n%s\nstderr:\n%s", args, code, want, out.String(), errb.String())
	}
	return out.String() + errb.String()
}

// dump returns all points of a measurement as sorted line protocol.
func (e *env1) dump(m string) []string {
	e.t.Helper()
	ctx := context.Background()
	j := &Job{C: e.c, Log: io.Discard}
	s, err := j.Schema(ctx, m)
	if err != nil {
		e.t.Fatal(err)
	}
	var lines []string
	err = j.readWindow(ctx, m, s, -1<<62, 1<<62, false, func(p *Point) error {
		sort.Slice(p.Fields, func(a, b int) bool { return p.Fields[a].Key < p.Fields[b].Key })
		lines = append(lines, strings.TrimSuffix(string(AppendLine(nil, p)), "\n"))
		return nil
	})
	if err != nil {
		e.t.Fatal(err)
	}
	sort.Strings(lines)
	return lines
}

func (e *env1) fieldTypes(m string) map[string][]string {
	e.t.Helper()
	s, err := (&Job{C: e.c}).Schema(context.Background(), m)
	if err != nil {
		e.t.Fatal(err)
	}
	return s.FieldTypes
}

func (e *env1) equal(got []string, want ...string) {
	e.t.Helper()
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		e.t.Fatalf("got:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

const day = int64(24 * time.Hour)

func TestE2EInPlace(t *testing.T) {
	e := setup(t)
	m := e.name("cpu")
	// Three days, awkward keys and values, a series without the region tag,
	// an integer above 2^53 and a float that JSON shows as "2".
	e.write(fmt.Sprintf(`%[1]s,host=web\ 1,region=eu\,west usage=1.5,guest=0.5,count=9007199254740993i,note="say \"hi\"",up=true %[2]d
%[1]s,host=web\ 1,region=eu\,west usage=2,guest=0.25,count=3i,note="x",up=false %[3]d
%[1]s,host=db usage=3.25,guest=1,count=-4i,note="",up=true %[4]d
`, m, 1*day+5, 2*day+7, 3*day+11))
	out := e.run(0, "plan", "-m", m, "--rename-tag", "host=hostname", "--rename-field", "usage=usage_total",
		"--drop-field", "guest", "--tag-value", "host:db=database", "--scan")
	for _, s := range []string{`rename tag "host" -> "hostname"`, `drop field "guest"`, "scan: 3 points would become 3 points"} {
		if !strings.Contains(out, s) {
			t.Fatalf("plan output lacks %q:\n%s", s, out)
		}
	}
	e.run(0, "apply", "-m", m, "--rename-tag", "host=hostname", "--rename-field", "usage=usage_total",
		"--drop-field", "guest", "--tag-value", "host:db=database", "--window", "12h", "--yes")
	e.equal(e.dump(m),
		fmt.Sprintf(`%s,hostname=web\ 1,region=eu\,west count=9007199254740993i,note="say \"hi\"",up=true,usage_total=1.5 %d`, m, 1*day+5),
		fmt.Sprintf(`%s,hostname=web\ 1,region=eu\,west count=3i,note="x",up=false,usage_total=2 %d`, m, 2*day+7),
		fmt.Sprintf(`%s,hostname=database count=-4i,note="",up=true,usage_total=3.25 %d`, m, 3*day+11),
	)
	ft := e.fieldTypes(m)
	if _, ok := ft["guest"]; ok {
		t.Fatalf("dropped field still in SHOW FIELD KEYS: %v", ft)
	}
	if ft["usage_total"][0] != tFloat || ft["count"][0] != tInteger {
		t.Fatalf("field types changed: %v", ft)
	}
	if got := e.dump(m + "__reshape_tmp"); len(got) != 0 {
		t.Fatalf("staging measurement left behind: %v", got)
	}
}

func TestE2ETypeConflict(t *testing.T) {
	e := setup(t)
	m := e.name("temp")
	// Shard groups are 7 days by default; the conflict needs two of them.
	e.write(fmt.Sprintf("%[1]s,s=a v=1.5 %[2]d\n%[1]s,s=a v=2.5 %[3]d\n", m, 1*day, 2*day))
	e.write(fmt.Sprintf("%[1]s,s=a v=7i %[2]d\n", m, 20*day))
	e.write(fmt.Sprintf("%[1]s,s=a v=\"12.5\" %[2]d\n", m, 40*day))
	if got := e.fieldTypes(m)["v"]; len(got) != 3 {
		t.Skipf("server did not create a type conflict: %v", got)
	}
	out := e.run(1, "plan", "-m", m)
	if !strings.Contains(out, `field "v" has several types`) || !strings.Contains(out, "SELECT * returns only one") {
		t.Fatalf("conflict not reported:\n%s", out)
	}
	// SELECT * shows only the float values: the reason SELECT INTO loses data.
	n := 0
	e.c.Query(context.Background(), "SELECT * FROM "+ident(m), func(_ int, s Series) error { n += len(s.Values); return nil })
	if n >= 4 {
		t.Fatalf("SELECT * returned all %d points; expected it to miss the other types", n)
	}
	t.Logf("SELECT * returned %d of 4 points", n)
	e.run(1, "apply", "-m", m, "--cast", "v=integer", "--yes") // 1.5 is not a whole number
	if got := e.dump(m); len(got) != 4 {
		t.Fatalf("source changed after a failed cast: %v", got)
	}
	e.c.DropMeasurement(context.Background(), m+"__reshape_tmp")
	e.run(0, "apply", "-m", m, "--cast", "v=float", "--yes")
	e.equal(e.dump(m),
		fmt.Sprintf("%s,s=a v=1.5 %d", m, 1*day),
		fmt.Sprintf("%s,s=a v=2.5 %d", m, 2*day),
		fmt.Sprintf("%s,s=a v=7 %d", m, 20*day),
		fmt.Sprintf("%s,s=a v=12.5 %d", m, 40*day),
	)
	if got := e.fieldTypes(m)["v"]; len(got) != 1 || got[0] != tFloat {
		t.Fatalf("SHOW FIELD KEYS after cast: %v", got)
	}
}

func TestE2ERenameMeasurementAndBackup(t *testing.T) {
	e := setup(t)
	m, m2 := e.name("old"), e.name("new")
	e.write(fmt.Sprintf("%[1]s,k=v a=1i %[2]d\n%[1]s,k=w a=2i,b=\"s\" %[3]d\n", m, day, 9*day))
	backup := filepath.Join(t.TempDir(), "old.lp.gz")
	e.run(0, "apply", "-m", m, "--rename-measurement", m2, "--backup", backup, "--yes")
	if got := e.dump(m); len(got) != 0 {
		t.Fatalf("old measurement still has data: %v", got)
	}
	e.equal(e.dump(m2),
		fmt.Sprintf("%s,k=v a=1i %d", m2, day),
		fmt.Sprintf(`%s,k=w a=2i,b="s" %d`, m2, 9*day))
	if fi, err := os.Stat(backup); err != nil || fi.Size() == 0 {
		t.Fatalf("backup: %v", err)
	}
	// Restoring the backup through the write API gives the original back.
	b, _ := readGzip(backup)
	var lp []string
	for _, l := range strings.Split(string(b), "\n") {
		if l != "" && !strings.HasPrefix(l, "#") {
			lp = append(lp, l)
		}
	}
	e.write(strings.Join(lp, "\n"))
	e.equal(e.dump(m),
		fmt.Sprintf("%s,k=v a=1i %d", m, day),
		fmt.Sprintf(`%s,k=w a=2i,b="s" %d`, m, 9*day))
}

func TestE2EBackupOfConflict(t *testing.T) {
	e := setup(t)
	m := e.name("bk")
	e.write(fmt.Sprintf("%[1]s v=20 %[2]d\n%[1]s v=1.5 %[3]d\n", m, day, 2*day))
	e.write(fmt.Sprintf("%[1]s v=7i %[2]d\n", m, 20*day))
	backup := filepath.Join(t.TempDir(), "bk.lp.gz")
	e.run(0, "apply", "-m", m, "--cast", "v=float", "--backup", backup, "--yes")
	b, err := readGzip(backup)
	if err != nil {
		t.Fatal(err)
	}
	// InfluxQL cannot tell float 20 from integer 20; the backup uses floats
	// so that restoring it does not mix types within a shard.
	for _, want := range []string{fmt.Sprintf("%s v=20 %d\n", m, day), fmt.Sprintf("%s v=7 %d\n", m, 20*day)} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("backup lacks %q:\n%s", want, b)
		}
	}
	if err := e.c.DropMeasurement(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	e.write(string(b))
	e.equal(e.dump(m), fmt.Sprintf("%s v=20 %d", m, day), fmt.Sprintf("%s v=1.5 %d", m, 2*day), fmt.Sprintf("%s v=7 %d", m, 20*day))
}

func TestE2ECollisions(t *testing.T) {
	e := setup(t)
	m := e.name("net")
	e.write(fmt.Sprintf("%[1]s,host=a,if=eth0 rx=1i %[2]d\n%[1]s,host=a,if=eth1 rx=2i %[2]d\n%[1]s,host=a,if=eth1 tx=5i %[3]d\n", m, day, 2*day))
	out := e.run(1, "plan", "-m", m, "--drop-tag", "if", "--scan")
	if !strings.Contains(out, "collision") {
		t.Fatalf("collision not found by plan --scan:\n%s", out)
	}
	e.run(1, "apply", "-m", m, "--drop-tag", "if", "--yes")
	if got := e.dump(m); len(got) != 3 {
		t.Fatalf("source changed after a refused apply: %v", got)
	}
	e.c.DropMeasurement(context.Background(), m+"__reshape_tmp")
	e.run(0, "apply", "-m", m, "--drop-tag", "if", "--allow-collisions", "--yes")
	e.equal(e.dump(m),
		fmt.Sprintf("%s,host=a rx=2i %d", m, day),
		fmt.Sprintf("%s,host=a tx=5i %d", m, 2*day))
}

func TestE2EResume(t *testing.T) {
	e := setup(t)
	m := e.name("res")
	e.write(fmt.Sprintf("%[1]s v=\"1\" %[2]d\n%[1]s v=\"2\" %[3]d\n%[1]s v=\"oops\" %[4]d\n%[1]s v=\"4\" %[5]d\n", m, day, 2*day, 3*day, 4*day))
	state := filepath.Join(t.TempDir(), "state.json")
	args := append([]string{"apply"}, e.args...)
	args = append(args, "-m", m, "--cast", "v=integer", "--state", state, "--yes")
	var out bytes.Buffer
	if code := run(args, &out, &out); code != 1 || !strings.Contains(out.String(), "string oops is not a whole number") {
		t.Fatalf("exit %d:\n%s", code, out.String())
	}
	st, err := os.ReadFile(state)
	if err != nil || !strings.Contains(string(st), fmt.Sprintf(`"next": %d`, 3*day)) {
		t.Fatalf("state after failure: %s %v", st, err)
	}
	e.write(fmt.Sprintf("%s v=\"3\" %d\n", m, 3*day)) // fix the bad value
	out.Reset()
	if code := run(args, &out, &out); code != 0 || !strings.Contains(out.String(), "resuming at step 1") {
		t.Fatalf("resume: exit %d:\n%s", code, out.String())
	}
	e.equal(e.dump(m),
		fmt.Sprintf("%s v=1i %d", m, day), fmt.Sprintf("%s v=2i %d", m, 2*day),
		fmt.Sprintf("%s v=3i %d", m, 3*day), fmt.Sprintf("%s v=4i %d", m, 4*day))
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatalf("state file left after success: %v", err)
	}
}

func TestE2ERefusals(t *testing.T) {
	e := setup(t)
	m := e.name("ref")
	e.write(fmt.Sprintf("%s,h=a v=1 %d\n", m, day))
	e.write(fmt.Sprintf("%s__reshape_tmp,h=a v=1 %d\n", m, day))
	out := e.run(1, "apply", "-m", m, "--rename-tag", "h=host", "--yes")
	if !strings.Contains(out, "already has data") {
		t.Fatalf("non-empty staging measurement not refused:\n%s", out)
	}
	e.c.DropMeasurement(context.Background(), m+"__reshape_tmp")
	if out := e.run(2, "apply", "-m", m, "--rename-tag", "h=host"); !strings.Contains(out, "--yes") {
		t.Fatalf("apply without --yes:\n%s", out)
	}
	if e.c.Major != 1 {
		return
	}
	// 1.x: DROP MEASUREMENT works on all retention policies of a database.
	ctx := context.Background()
	if err := e.c.Query(ctx, "CREATE RETENTION POLICY other ON "+ident(e.c.DB)+" DURATION 0s REPLICATION 1", func(int, Series) error { return nil }); err != nil {
		t.Fatal(err)
	}
	params := url.Values{"db": {e.c.DB}, "rp": {"other"}, "precision": {"ns"}}
	resp, err := http.Post(e.c.URL+"/write?"+params.Encode(), "text/plain", strings.NewReader(fmt.Sprintf("%s,h=a v=2 %d\n", m, day)))
	if err != nil || resp.StatusCode != 204 {
		t.Fatalf("write to rp other: %v %v", resp, err)
	}
	out = e.run(1, "apply", "-m", m, "--rename-tag", "h=host", "--yes")
	if !strings.Contains(out, `retention policy "other"`) {
		t.Fatalf("data in another retention policy not refused:\n%s", out)
	}
	e.c.Query(ctx, "DROP RETENTION POLICY other ON "+ident(e.c.DB), func(int, Series) error { return nil })
}

func readGzip(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(zr)
}

func TestE2EVolume(t *testing.T) {
	e := setup(t)
	m := e.name("vol")
	// 24000 points in 40 series over 5 days: several chunks per query and
	// several write batches per window.
	var b strings.Builder
	for i := 0; i < 24000; i++ {
		fmt.Fprintf(&b, "%s,host=h%d,dc=x%d v=%di,w=%d.5 %d\n", m, i%40, i%3, i, i, day+int64(i)*int64(18*time.Second))
		if (i+1)%6000 == 0 {
			e.write(b.String())
			b.Reset()
		}
	}
	out := e.run(0, "apply", "-m", m, "--rename-field", "v=value", "--drop-tag", "dc", "--batch", "3000", "--yes")
	if !strings.Contains(out, "24000 points read, 24000 written, 48000 values verified") {
		t.Fatalf("unexpected output:\n%s", out)
	}
	got := e.dump(m)
	if len(got) != 24000 {
		t.Fatalf("got %d points", len(got))
	}
	want := fmt.Sprintf("%s,host=h7 value=12007i,w=12007.5 %d", m, day+12007*int64(18*time.Second))
	found := false
	for _, l := range got {
		if l == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing %q", want)
	}
}

func TestE2EConvert(t *testing.T) {
	e := setup(t)
	m := e.name("conv")
	e.write(fmt.Sprintf(`%[1]s,host=a,unit=C temp=20.5,status="ok",code=3i %[2]d
%[1]s,host=a,unit=F temp=70,status="warn",code=4i %[3]d
%[1]s,host=b temp=19,code=3i %[4]d
`, m, day, 2*day, 3*day))
	e.run(0, "apply", "-m", m, "--tag-to-field", "unit", "--field-to-tag", "status", "--field-to-tag", "code", "--yes")
	e.equal(e.dump(m),
		fmt.Sprintf(`%s,code=3,host=a,status=ok temp=20.5,unit="C" %d`, m, day),
		fmt.Sprintf(`%s,code=4,host=a,status=warn temp=70,unit="F" %d`, m, 2*day),
		fmt.Sprintf(`%s,code=3,host=b temp=19 %d`, m, 3*day),
	)
	s, err := (&Job{C: e.c}).Schema(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(s.Tags, ",") != "code,host,status" || s.FieldTypes["unit"][0] != tString || len(s.Fields) != 2 {
		t.Fatalf("schema after conversion: %+v", s)
	}
	// A point whose only field would become a tag is refused before writing.
	m2 := e.name("conv2")
	e.write(fmt.Sprintf("%[1]s,host=a v=1i,s=\"x\" %[2]d\n%[1]s,host=b s=\"y\" %[3]d\n", m2, day, 2*day))
	out := e.run(1, "plan", "-m", m2, "--field-to-tag", "s", "--scan")
	if !strings.Contains(out, "no field left after turning fields into tags") {
		t.Fatalf("plan --scan output:\n%s", out)
	}
}
