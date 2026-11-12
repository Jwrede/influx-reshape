package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"
)

const usage = `influx-reshape: rename, drop and retype tags, fields and measurements in InfluxDB 1.x and 2.x

usage:
  influx-reshape plan  [connection] -m MEASUREMENT [changes] [--scan]
  influx-reshape apply [connection] -m MEASUREMENT [changes] --yes

connection:
  --url URL            default http://localhost:8086 (env INFLUX_URL)
  --db NAME            database (1.x) or bucket / DBRP database (2.x)
  --rp NAME            retention policy (default: the database default)
  --user, --password   1.x credentials (env INFLUX_USERNAME, INFLUX_PASSWORD)
  --token, --org       2.x token and organization (env INFLUX_TOKEN, INFLUX_ORG)

changes (repeatable):
  --rename-measurement NEW
  --rename-tag OLD=NEW        --drop-tag KEY       --tag-value KEY:OLD=NEW
  --rename-field OLD=NEW      --drop-field KEY     --cast FIELD=TYPE
                              (TYPE: float, integer, unsigned, string, boolean)
  --tag-to-field KEY          --field-to-tag KEY

apply:
  --yes                required; apply drops and rewrites data
  --backup FILE.gz     write the original points as gzipped line protocol first
  --state FILE         progress file for resuming (default: influx-reshape-<db>-<measurement>.state.json)
  --window DURATION    time window per copy and verification (default 24h)
  --batch N            points per write request (default 5000)
  --allow-collisions   accept that points merging into one series keep only the last value
`

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(s string) error { *m = append(*m, s); return nil }

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprint(stdout, usage)
		return 0
	}
	cmd := args[0]
	if cmd != "plan" && cmd != "apply" {
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", cmd, usage)
		return 2
	}
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	c := &Client{HTTP: &http.Client{Timeout: 10 * time.Minute}}
	fs.StringVar(&c.URL, "url", env("INFLUX_URL", "http://localhost:8086"), "")
	fs.StringVar(&c.DB, "db", "", "")
	fs.StringVar(&c.RP, "rp", "", "")
	fs.StringVar(&c.User, "user", os.Getenv("INFLUX_USERNAME"), "")
	fs.StringVar(&c.Password, "password", os.Getenv("INFLUX_PASSWORD"), "")
	fs.StringVar(&c.Token, "token", os.Getenv("INFLUX_TOKEN"), "")
	fs.StringVar(&c.Org, "org", os.Getenv("INFLUX_ORG"), "")
	var m, renameM, state, backup string
	var renameTag, dropTag, tagValue, renameField, dropField, cast, tagToField, fieldToTag multi
	var yes, scan, allowCollisions bool
	var window time.Duration
	var batch int
	fs.StringVar(&m, "m", "", "")
	fs.StringVar(&m, "measurement", "", "")
	fs.StringVar(&renameM, "rename-measurement", "", "")
	fs.Var(&renameTag, "rename-tag", "")
	fs.Var(&dropTag, "drop-tag", "")
	fs.Var(&tagValue, "tag-value", "")
	fs.Var(&renameField, "rename-field", "")
	fs.Var(&dropField, "drop-field", "")
	fs.Var(&cast, "cast", "")
	fs.Var(&tagToField, "tag-to-field", "")
	fs.Var(&fieldToTag, "field-to-tag", "")
	fs.BoolVar(&yes, "yes", false, "")
	fs.BoolVar(&scan, "scan", false, "")
	fs.BoolVar(&allowCollisions, "allow-collisions", false, "")
	fs.StringVar(&state, "state", "", "")
	fs.StringVar(&backup, "backup", "", "")
	fs.DurationVar(&window, "window", 24*time.Hour, "")
	fs.IntVar(&batch, "batch", 5000, "")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	r, err := buildRules(renameM, renameTag, dropTag, tagValue, renameField, dropField, cast)
	if err == nil {
		for _, k := range tagToField {
			r.TagToField[k] = true
		}
		for _, k := range fieldToTag {
			r.FieldToTag[k] = true
		}
	}
	if err == nil && (c.DB == "" || m == "") {
		err = errors.New("--db and -m are required")
	}
	if err == nil && window <= 0 {
		err = errors.New("--window must be positive")
	}
	if err == nil && batch <= 0 {
		err = errors.New("--batch must be positive")
	}
	if err == nil && r.RenameMeasurement == m {
		err = errors.New("--rename-measurement is the current name")
	}
	if err == nil && cmd == "apply" && r.Empty() {
		err = errors.New("no changes given")
	}
	if err == nil && cmd == "apply" && !yes {
		err = errors.New("apply rewrites and drops data; run plan first, then add --yes")
	}
	if err != nil {
		fmt.Fprintln(stderr, "influx-reshape:", err)
		return 2
	}
	if state == "" {
		state = fmt.Sprintf("influx-reshape-%s-%s.state.json", safe(c.DB), safe(m))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	j := &Job{C: c, Src: m, Rules: r, Window: window, Batch: batch, StatePath: state, BackupPath: backup,
		AllowCollisions: allowCollisions, Log: stdout}
	if err := c.Ping(ctx); err != nil {
		fmt.Fprintln(stderr, "influx-reshape:", err)
		return 1
	}
	if err := j.ResolveRP(ctx); err != nil {
		fmt.Fprintln(stderr, "influx-reshape:", err)
		return 1
	}
	if cmd == "plan" {
		err = j.Plan(ctx, stdout, scan)
	} else {
		err = j.Apply(ctx)
	}
	if err != nil {
		fmt.Fprintln(stderr, "influx-reshape:", err)
		if cmd == "apply" {
			fmt.Fprintf(stderr, "progress is saved in %s; fix the cause and run the same command again to continue\n", state)
		}
		return 1
	}
	return 0
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func safe(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, s)
}

func splitPair(s, sep, flagName string) (string, string, error) {
	i := strings.Index(s, sep)
	if i <= 0 || i == len(s)-len(sep) {
		return "", "", fmt.Errorf("--%s %q: want OLD%sNEW", flagName, s, sep)
	}
	return s[:i], s[i+len(sep):], nil
}

func buildRules(renameM string, renameTag, dropTag, tagValue, renameField, dropField, cast []string) (*Rules, error) {
	r := NewRules()
	r.RenameMeasurement = renameM
	for _, s := range renameTag {
		a, b, err := splitPair(s, "=", "rename-tag")
		if err != nil {
			return nil, err
		}
		r.RenameTag[a] = b
	}
	for _, s := range dropTag {
		r.DropTag[s] = true
	}
	for _, s := range tagValue {
		i := strings.Index(s, ":")
		if i <= 0 {
			return nil, fmt.Errorf("--tag-value %q: want KEY:OLD=NEW", s)
		}
		eq := strings.Index(s[i+1:], "=")
		if eq < 0 {
			return nil, fmt.Errorf("--tag-value %q: want KEY:OLD=NEW", s)
		}
		k, old, nv := s[:i], s[i+1:i+1+eq], s[i+2+eq:]
		if old == "" {
			return nil, fmt.Errorf("--tag-value %q: OLD must not be empty", s)
		}
		if r.TagValue[k] == nil {
			r.TagValue[k] = map[string]string{}
		}
		r.TagValue[k][old] = nv
	}
	for _, s := range renameField {
		a, b, err := splitPair(s, "=", "rename-field")
		if err != nil {
			return nil, err
		}
		r.RenameField[a] = b
	}
	for _, s := range dropField {
		r.DropField[s] = true
	}
	for _, s := range cast {
		a, b, err := splitPair(s, "=", "cast")
		if err != nil {
			return nil, err
		}
		r.Cast[a] = b
	}
	return r, nil
}
