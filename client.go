package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks to InfluxDB 1.x natively and to InfluxDB 2.x through its v1
// compatibility API (/query, /write). Deletes on 2.x go through
// /api/v2/delete, because 2.x InfluxQL is read only.
type Client struct {
	URL      string
	DB       string
	RP       string
	User     string
	Password string
	Token    string
	Org      string

	HTTP    *http.Client
	Version string
	Major   int
}

// Series is one series of an InfluxQL result.
type Series struct {
	Name    string            `json:"name"`
	Tags    map[string]string `json:"tags"`
	Columns []string          `json:"columns"`
	Values  [][]any           `json:"values"`
}

type queryResponse struct {
	Results []struct {
		StatementID int      `json:"statement_id"`
		Series      []Series `json:"series"`
		Error       string   `json:"error"`
	} `json:"results"`
	Error string `json:"error"`
}

func (c *Client) auth(req *http.Request) {
	switch {
	case c.Token != "":
		req.Header.Set("Authorization", "Token "+c.Token)
	case c.User != "":
		req.SetBasicAuth(c.User, c.Password)
	}
}

func (c *Client) do(ctx context.Context, method, path string, params url.Values, body io.Reader, ctype string) (*http.Response, error) {
	u := strings.TrimRight(c.URL, "/") + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	c.auth(req)
	return c.HTTP.Do(req)
}

// Ping reads the server version.
func (c *Client) Ping(ctx context.Context) error {
	resp, err := c.do(ctx, "GET", "/ping", nil, nil, "")
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("ping: HTTP %d", resp.StatusCode)
	}
	c.Version = resp.Header.Get("X-Influxdb-Version")
	v := strings.TrimPrefix(c.Version, "v")
	if i := strings.IndexByte(v, '.'); i > 0 {
		c.Major, _ = strconv.Atoi(v[:i])
	}
	if c.Major != 1 && c.Major != 2 {
		return fmt.Errorf("unsupported InfluxDB version %q (need 1.x or 2.x)", c.Version)
	}
	return nil
}

// Query runs InfluxQL and calls fn for every series of every statement,
// streaming chunked responses. Numbers stay json.Number so that integers
// above 2^53 keep their exact value.
func (c *Client) Query(ctx context.Context, q string, fn func(stmt int, s Series) error) error {
	params := url.Values{"db": {c.DB}, "q": {q}, "epoch": {"ns"}, "chunked": {"true"}, "chunk_size": {"10000"}}
	if c.RP != "" {
		params.Set("rp", c.RP)
	}
	method := "GET"
	if !isRead(q) {
		method = "POST"
	}
	resp, err := c.do(ctx, method, "/query", params, nil, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("query %q: HTTP %d: %s", short(q), resp.StatusCode, bytes.TrimSpace(b))
	}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	for {
		var r queryResponse
		if err := dec.Decode(&r); err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("query %q: %w", short(q), err)
		}
		if r.Error != "" {
			return fmt.Errorf("query %q: %s", short(q), r.Error)
		}
		for _, res := range r.Results {
			if res.Error != "" {
				return fmt.Errorf("query %q: %s", short(q), res.Error)
			}
			for _, s := range res.Series {
				if err := fn(res.StatementID, s); err != nil {
					return err
				}
			}
		}
	}
}

func isRead(q string) bool {
	u := strings.ToUpper(strings.TrimSpace(q))
	return strings.HasPrefix(u, "SELECT") || strings.HasPrefix(u, "SHOW")
}

func short(q string) string {
	if len(q) > 160 {
		return q[:160] + "..."
	}
	return q
}

// Write sends line protocol with nanosecond timestamps.
func (c *Client) Write(ctx context.Context, lines []byte) error {
	params := url.Values{"db": {c.DB}, "precision": {"ns"}}
	if c.RP != "" {
		params.Set("rp", c.RP)
	}
	var last error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
		resp, err := c.do(ctx, "POST", "/write", params, bytes.NewReader(lines), "text/plain; charset=utf-8")
		if err != nil {
			last = err
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			return nil
		}
		last = fmt.Errorf("write: HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(b))
		if resp.StatusCode/100 == 4 {
			return last
		}
	}
	return last
}

// DropMeasurement removes all data of a measurement. On 1.x this uses DROP
// MEASUREMENT, which also removes the measurement's field keys and types.
func (c *Client) DropMeasurement(ctx context.Context, m string) error {
	if c.Major == 1 {
		return c.Query(ctx, "DROP MEASUREMENT "+ident(m), func(int, Series) error { return nil })
	}
	orgID, bucketID, err := c.bucket(ctx)
	if err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]string{
		"start":     "1677-09-21T00:12:43.145224194Z",
		"stop":      "2262-04-11T23:47:16.854775806Z",
		"predicate": `_measurement="` + strings.ReplaceAll(strings.ReplaceAll(m, `\`, `\\`), `"`, `\"`) + `"`,
	})
	resp, err := c.do(ctx, "POST", "/api/v2/delete", url.Values{"orgID": {orgID}, "bucketID": {bucketID}}, bytes.NewReader(body), "application/json")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("delete measurement %q: HTTP %d: %s", m, resp.StatusCode, bytes.TrimSpace(b))
	}
	return nil
}

// bucket resolves the 2.x bucket behind --db/--rp via the DBRP mappings,
// falling back to a bucket named like the database.
func (c *Client) bucket(ctx context.Context) (orgID, bucketID string, err error) {
	if c.Org == "" {
		return "", "", fmt.Errorf("InfluxDB 2.x needs --org (or INFLUX_ORG) to delete data")
	}
	var dbrps struct {
		Content []struct {
			OrgID    string `json:"orgID"`
			BucketID string `json:"bucketID"`
			RP       string `json:"retention_policy"`
			Default  bool   `json:"default"`
		} `json:"content"`
	}
	if err := c.getJSON(ctx, "/api/v2/dbrps", url.Values{"org": {c.Org}, "db": {c.DB}}, &dbrps); err == nil {
		for _, d := range dbrps.Content {
			if (c.RP == "" && d.Default) || (c.RP != "" && d.RP == c.RP) {
				return d.OrgID, d.BucketID, nil
			}
		}
	}
	var buckets struct {
		Buckets []struct {
			ID    string `json:"id"`
			OrgID string `json:"orgID"`
		} `json:"buckets"`
	}
	if err := c.getJSON(ctx, "/api/v2/buckets", url.Values{"org": {c.Org}, "name": {c.DB}}, &buckets); err != nil {
		return "", "", err
	}
	if len(buckets.Buckets) != 1 {
		return "", "", fmt.Errorf("no DBRP mapping and no bucket named %q in org %q", c.DB, c.Org)
	}
	return buckets.Buckets[0].OrgID, buckets.Buckets[0].ID, nil
}

func (c *Client) getJSON(ctx context.Context, path string, params url.Values, v any) error {
	resp, err := c.do(ctx, "GET", path, params, nil, "")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("GET %s: HTTP %d: %s", path, resp.StatusCode, bytes.TrimSpace(b))
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

// ident quotes an InfluxQL identifier.
func ident(s string) string {
	return `"` + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`) + `"`
}

// from returns the FROM clause for a measurement in the client's db/rp.
func (c *Client) from(m string) string {
	if c.RP != "" {
		return ident(c.DB) + "." + ident(c.RP) + "." + ident(m)
	}
	return ident(m)
}
