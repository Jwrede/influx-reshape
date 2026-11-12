package main

import (
	"fmt"
	"sort"
	"strings"
)

// Rules describe how points of one measurement are rewritten.
type Rules struct {
	RenameMeasurement string
	RenameTag         map[string]string            // old key -> new key
	DropTag           map[string]bool              // source key
	TagValue          map[string]map[string]string // source key -> old value -> new value
	RenameField       map[string]string            // old key -> new key
	DropField         map[string]bool
	Cast              map[string]string // source field key -> type
}

func NewRules() *Rules {
	return &Rules{
		RenameTag:   map[string]string{},
		DropTag:     map[string]bool{},
		TagValue:    map[string]map[string]string{},
		RenameField: map[string]string{},
		DropField:   map[string]bool{},
		Cast:        map[string]string{},
	}
}

func (r *Rules) Empty() bool {
	return r.RenameMeasurement == "" && len(r.RenameTag) == 0 && len(r.DropTag) == 0 && len(r.TagValue) == 0 &&
		len(r.RenameField) == 0 && len(r.DropField) == 0 && len(r.Cast) == 0
}

// MergesSeries reports whether two source points can end up with the same
// series key and timestamp, so the copy must look for collisions.
func (r *Rules) MergesSeries() bool {
	return len(r.DropTag) > 0 || len(r.TagValue) > 0
}

// Describe lists the rules in a stable order, one per line.
func (r *Rules) Describe(src string) []string {
	var out []string
	if r.RenameMeasurement != "" {
		out = append(out, fmt.Sprintf("rename measurement %q -> %q", src, r.RenameMeasurement))
	}
	for _, k := range sortedKeys(r.RenameTag) {
		out = append(out, fmt.Sprintf("rename tag %q -> %q", k, r.RenameTag[k]))
	}
	for _, k := range sortedKeys(r.DropTag) {
		out = append(out, fmt.Sprintf("drop tag %q", k))
	}
	for _, k := range sortedKeys(r.TagValue) {
		for _, v := range sortedKeys(r.TagValue[k]) {
			out = append(out, fmt.Sprintf("set tag %q value %q -> %q", k, v, r.TagValue[k][v]))
		}
	}
	for _, k := range sortedKeys(r.RenameField) {
		out = append(out, fmt.Sprintf("rename field %q -> %q", k, r.RenameField[k]))
	}
	for _, k := range sortedKeys(r.DropField) {
		out = append(out, fmt.Sprintf("drop field %q", k))
	}
	for _, k := range sortedKeys(r.Cast) {
		out = append(out, fmt.Sprintf("cast field %q to %s", k, r.Cast[k]))
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Schema is what SHOW FIELD KEYS and SHOW TAG KEYS report for a measurement.
type Schema struct {
	Fields     []string            // in the order the server lists them
	FieldTypes map[string][]string // a field has several types after a type conflict
	Tags       []string
}

func (s *Schema) hasTag(k string) bool {
	for _, t := range s.Tags {
		if t == k {
			return true
		}
	}
	return false
}

// Check validates the rules against the schema: every rule must refer to an
// existing key, renames must not land on keys that stay, and fields with
// several types need a cast.
func (r *Rules) Check(s *Schema) error {
	var errs []string
	add := func(format string, a ...any) { errs = append(errs, fmt.Sprintf(format, a...)) }
	for _, k := range sortedKeys(r.RenameTag) {
		if !s.hasTag(k) {
			add("rename tag: no tag %q", k)
		}
		if r.DropTag[k] {
			add("tag %q is both renamed and dropped", k)
		}
	}
	for _, k := range sortedKeys(r.DropTag) {
		if !s.hasTag(k) {
			add("drop tag: no tag %q", k)
		}
	}
	for _, k := range sortedKeys(r.TagValue) {
		if !s.hasTag(k) {
			add("set tag value: no tag %q", k)
		}
		if r.DropTag[k] {
			add("tag %q is both dropped and given new values", k)
		}
	}
	for _, k := range sortedKeys(r.RenameField) {
		if _, ok := s.FieldTypes[k]; !ok {
			add("rename field: no field %q", k)
		}
		if r.DropField[k] {
			add("field %q is both renamed and dropped", k)
		}
	}
	for _, k := range sortedKeys(r.DropField) {
		if _, ok := s.FieldTypes[k]; !ok {
			add("drop field: no field %q", k)
		}
		if _, ok := r.Cast[k]; ok {
			add("field %q is both dropped and cast", k)
		}
	}
	for _, k := range sortedKeys(r.Cast) {
		if _, ok := s.FieldTypes[k]; !ok {
			add("cast: no field %q", k)
		}
		switch r.Cast[k] {
		case tFloat, tInteger, tUnsigned, tString, tBoolean:
		default:
			add("cast: unknown type %q (float, integer, unsigned, string, boolean)", r.Cast[k])
		}
	}
	for _, f := range s.Fields {
		if len(s.FieldTypes[f]) > 1 && !r.DropField[f] && r.Cast[f] == "" {
			add("field %q has several types (%s); add --cast %s=<type> or --drop-field %s",
				f, strings.Join(s.FieldTypes[f], ", "), f, f)
		}
	}
	// Output keys must be unique among tags and among fields. A tag and a
	// field may already share a key in existing data; only a rename that
	// creates such a pair is refused.
	tagOwner, fieldOwner := map[string]string{}, map[string]string{}
	for _, t := range s.Tags {
		if r.DropTag[t] {
			continue
		}
		out := r.tagKey(t)
		if prev, ok := tagOwner[out]; ok {
			add("tags %q and %q would both be called %q", prev, t, out)
		}
		tagOwner[out] = t
	}
	for _, f := range s.Fields {
		if r.DropField[f] {
			continue
		}
		out := r.fieldKey(f)
		if prev, ok := fieldOwner[out]; ok {
			add("fields %q and %q would both be called %q", prev, f, out)
		}
		fieldOwner[out] = f
		if t, ok := tagOwner[out]; ok && (t != out || f != out) {
			add("tag %q and field %q would both be called %q", t, f, out)
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "\n"))
	}
	return nil
}

func (r *Rules) tagKey(k string) string {
	if n, ok := r.RenameTag[k]; ok {
		return n
	}
	return k
}

func (r *Rules) fieldKey(k string) string {
	if n, ok := r.RenameField[k]; ok {
		return n
	}
	return k
}

// OutputFields maps every kept source field to its output key and type.
func (r *Rules) OutputFields(s *Schema) map[string]string {
	out := map[string]string{}
	for _, f := range s.Fields {
		if r.DropField[f] {
			continue
		}
		typ := s.FieldTypes[f][0]
		if c := r.Cast[f]; c != "" {
			typ = c
		}
		out[r.fieldKey(f)] = typ
	}
	return out
}

// Apply rewrites one point. It returns false if no field is left, in which
// case the point disappears (only possible when fields are dropped).
func (r *Rules) Apply(p *Point, measurement string) (bool, error) {
	p.Measurement = measurement
	if len(r.DropTag)+len(r.RenameTag)+len(r.TagValue) > 0 {
		tags := make(map[string]string, len(p.Tags))
		for k, v := range p.Tags {
			if v == "" || r.DropTag[k] {
				continue
			}
			if m, ok := r.TagValue[k]; ok {
				if nv, ok := m[v]; ok {
					v = nv
				}
			}
			if v == "" {
				continue
			}
			tags[r.tagKey(k)] = v
		}
		p.Tags = tags
	}
	fields := p.Fields[:0]
	for _, f := range p.Fields {
		if r.DropField[f.Key] {
			continue
		}
		if to := r.Cast[f.Key]; to != "" {
			v, err := Cast(f.Value, to)
			if err != nil {
				return false, fmt.Errorf("field %q at %s: %w", f.Key, p.Time, err)
			}
			f.Value = v
		}
		f.Key = r.fieldKey(f.Key)
		fields = append(fields, f)
	}
	p.Fields = fields
	return len(fields) > 0, nil
}

// seriesKey identifies a series independent of tag order.
func seriesKey(p *Point) string {
	keys := sortedKeys(p.Tags)
	var b strings.Builder
	b.WriteString(p.Measurement)
	for _, k := range keys {
		if p.Tags[k] == "" {
			continue
		}
		b.WriteByte(0)
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(p.Tags[k])
	}
	return b.String()
}
