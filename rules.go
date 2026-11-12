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
	TagToField        map[string]bool   // tag becomes a string field with the same key
	FieldToTag        map[string]bool   // field becomes a tag with the same key
}

func NewRules() *Rules {
	return &Rules{
		RenameTag:   map[string]string{},
		DropTag:     map[string]bool{},
		TagValue:    map[string]map[string]string{},
		RenameField: map[string]string{},
		DropField:   map[string]bool{},
		Cast:        map[string]string{},
		TagToField:  map[string]bool{},
		FieldToTag:  map[string]bool{},
	}
}

func (r *Rules) Empty() bool {
	return r.RenameMeasurement == "" && len(r.RenameTag) == 0 && len(r.DropTag) == 0 && len(r.TagValue) == 0 &&
		len(r.RenameField) == 0 && len(r.DropField) == 0 && len(r.Cast) == 0 &&
		len(r.TagToField) == 0 && len(r.FieldToTag) == 0
}

// MergesSeries reports whether two source points can end up with the same
// series key and timestamp, so the copy must look for collisions.
func (r *Rules) MergesSeries() bool {
	return len(r.DropTag) > 0 || len(r.TagValue) > 0 || len(r.TagToField) > 0
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
	for _, k := range sortedKeys(r.TagToField) {
		out = append(out, fmt.Sprintf("turn tag %q into a string field", k))
	}
	for _, k := range sortedKeys(r.FieldToTag) {
		out = append(out, fmt.Sprintf("turn field %q into a tag", k))
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
		if len(s.FieldTypes[f]) > 1 && !r.DropField[f] && r.Cast[f] == "" && !r.FieldToTag[f] {
			add("field %q has several types (%s); add --cast %s=<type> or --drop-field %s",
				f, strings.Join(s.FieldTypes[f], ", "), f, f)
		}
	}
	for _, k := range sortedKeys(r.TagToField) {
		if !s.hasTag(k) {
			add("tag to field: no tag %q", k)
		}
		if _, ok := r.RenameTag[k]; ok || r.DropTag[k] {
			add("tag %q is turned into a field and also renamed or dropped", k)
		}
	}
	for _, k := range sortedKeys(r.FieldToTag) {
		if _, ok := s.FieldTypes[k]; !ok {
			add("field to tag: no field %q", k)
		}
		if _, ok := r.RenameField[k]; ok || r.DropField[k] || r.Cast[k] != "" {
			add("field %q is turned into a tag and also renamed, dropped or cast", k)
		}
	}
	// Output keys must be unique among tags and among fields. A tag and a
	// field may already share a key in existing data; only a change that
	// creates such a pair is refused.
	type origin struct{ what, key string }
	tagOwner, fieldOwner := map[string]origin{}, map[string]origin{}
	claim := func(owner map[string]origin, kind, out string, o origin) {
		if prev, ok := owner[out]; ok {
			add("%s %q and %s %q would both become %s %q", prev.what, prev.key, o.what, o.key, kind, out)
			return
		}
		owner[out] = o
	}
	for _, t := range s.Tags {
		switch {
		case r.DropTag[t]:
		case r.TagToField[t]:
			claim(fieldOwner, "field", t, origin{"tag", t})
		default:
			claim(tagOwner, "tag", r.tagKey(t), origin{"tag", t})
		}
	}
	for _, f := range s.Fields {
		switch {
		case r.DropField[f]:
		case r.FieldToTag[f]:
			claim(tagOwner, "tag", f, origin{"field", f})
		default:
			claim(fieldOwner, "field", r.fieldKey(f), origin{"field", f})
		}
	}
	for _, out := range sortedKeys(tagOwner) {
		t, ok := fieldOwner[out]
		tg := tagOwner[out]
		if ok && !(tg.what == "tag" && tg.key == out && t.what == "field" && t.key == out) {
			add("%s %q and %s %q would both be called %q", tg.what, tg.key, t.what, t.key, out)
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
	for _, t := range s.Tags {
		if r.TagToField[t] {
			out[t] = tString
		}
	}
	for _, f := range s.Fields {
		if r.DropField[f] || r.FieldToTag[f] {
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
	var fromTags []FieldValue
	if len(r.DropTag)+len(r.RenameTag)+len(r.TagValue)+len(r.TagToField)+len(r.FieldToTag) > 0 {
		tags := make(map[string]string, len(p.Tags))
		for _, k := range sortedKeys(p.Tags) {
			v := p.Tags[k]
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
			if r.TagToField[k] {
				fromTags = append(fromTags, FieldValue{Key: k, Value: Value{Type: tString, Str: v}})
				continue
			}
			tags[r.tagKey(k)] = v
		}
		p.Tags = tags
	}
	fields := p.Fields[:0]
	converted := false
	for _, f := range p.Fields {
		if r.DropField[f.Key] {
			continue
		}
		if r.FieldToTag[f.Key] {
			if s := tagText(f.Value); s != "" {
				p.Tags[f.Key] = s
			}
			converted = true
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
	p.Fields = append(fields, fromTags...)
	if len(p.Fields) == 0 && converted {
		return false, fmt.Errorf("point at %s has no field left after turning fields into tags", p.Time)
	}
	return len(p.Fields) > 0, nil
}

// tagText formats a field value as a tag value.
func tagText(v Value) string {
	switch v.Type {
	case tString:
		return v.Str
	case tBoolean:
		return fmt.Sprint(v.Bool)
	}
	return v.Num
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
