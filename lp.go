package main

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// Field types as InfluxQL's SHOW FIELD KEYS reports them.
const (
	tFloat    = "float"
	tInteger  = "integer"
	tUnsigned = "unsigned"
	tString   = "string"
	tBoolean  = "boolean"
)

// Value is one field value with its storage type. Numbers are kept as their
// decimal text so nothing is rounded on the way through.
type Value struct {
	Type string
	Num  string // float, integer, unsigned
	Str  string
	Bool bool
}

type FieldValue struct {
	Key string
	Value
}

type Point struct {
	Measurement string
	Tags        map[string]string
	Fields      []FieldValue
	Time        string // nanoseconds since epoch
}

var (
	measurementEscaper = strings.NewReplacer(`\`, `\\`, ",", `\,`, " ", `\ `, "\n", `\n`)
	keyEscaper         = strings.NewReplacer(`\`, `\\`, ",", `\,`, "=", `\=`, " ", `\ `, "\n", `\n`)
	stringEscaper      = strings.NewReplacer(`\`, `\\`, `"`, `\"`)
)

// AppendLine appends the point as one line of line protocol. Empty tag
// values are skipped, as InfluxDB does not store them.
func AppendLine(b []byte, p *Point) []byte {
	b = append(b, measurementEscaper.Replace(p.Measurement)...)
	keys := make([]string, 0, len(p.Tags))
	for k, v := range p.Tags {
		if v != "" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		b = append(b, ',')
		b = append(b, keyEscaper.Replace(k)...)
		b = append(b, '=')
		b = append(b, keyEscaper.Replace(p.Tags[k])...)
	}
	for i, f := range p.Fields {
		if i == 0 {
			b = append(b, ' ')
		} else {
			b = append(b, ',')
		}
		b = append(b, keyEscaper.Replace(f.Key)...)
		b = append(b, '=')
		switch f.Type {
		case tFloat:
			b = append(b, f.Num...)
		case tInteger:
			b = append(b, f.Num...)
			b = append(b, 'i')
		case tUnsigned:
			b = append(b, f.Num...)
			b = append(b, 'u')
		case tString:
			b = append(b, '"')
			b = append(b, stringEscaper.Replace(f.Str)...)
			b = append(b, '"')
		case tBoolean:
			b = strconv.AppendBool(b, f.Bool)
		}
	}
	b = append(b, ' ')
	b = append(b, p.Time...)
	return append(b, '\n')
}

// decodeValue turns a JSON value from an InfluxQL result into a Value of
// the type the schema says the column has.
func decodeValue(v any, typ string) (Value, error) {
	switch typ {
	case tFloat, tInteger, tUnsigned:
		n, ok := v.(json.Number)
		if !ok {
			return Value{}, fmt.Errorf("expected a number for a %s field, got %T", typ, v)
		}
		s := n.String()
		if typ == tFloat {
			// JSON drops the type: 1.0 arrives as "1", which line protocol
			// also reads as a float, so the text can go out unchanged.
			if _, err := strconv.ParseFloat(s, 64); err != nil {
				return Value{}, err
			}
		}
		return Value{Type: typ, Num: s}, nil
	case tString:
		s, ok := v.(string)
		if !ok {
			return Value{}, fmt.Errorf("expected a string, got %T", v)
		}
		return Value{Type: typ, Str: s}, nil
	case tBoolean:
		b, ok := v.(bool)
		if !ok {
			return Value{}, fmt.Errorf("expected a boolean, got %T", v)
		}
		return Value{Type: typ, Bool: b}, nil
	}
	return Value{}, fmt.Errorf("unknown field type %q", typ)
}

// Cast converts a value to another field type. Conversions that would lose
// information (1.5 to integer, "abc" to float, 2 to boolean) are errors.
func Cast(v Value, to string) (Value, error) {
	if v.Type == to {
		return v, nil
	}
	switch to {
	case tString:
		switch v.Type {
		case tBoolean:
			return Value{Type: to, Str: strconv.FormatBool(v.Bool)}, nil
		default:
			return Value{Type: to, Str: v.Num}, nil
		}
	case tFloat:
		switch v.Type {
		case tInteger, tUnsigned:
			return Value{Type: to, Num: v.Num}, nil
		case tBoolean:
			if v.Bool {
				return Value{Type: to, Num: "1"}, nil
			}
			return Value{Type: to, Num: "0"}, nil
		case tString:
			f, err := strconv.ParseFloat(strings.TrimSpace(v.Str), 64)
			if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
				return Value{}, fmt.Errorf("string %q is not a number", v.Str)
			}
			return Value{Type: to, Num: strconv.FormatFloat(f, 'g', -1, 64)}, nil
		}
	case tInteger, tUnsigned:
		var s string
		switch v.Type {
		case tFloat, tInteger, tUnsigned:
			s = v.Num
		case tString:
			s = strings.TrimSpace(v.Str)
		case tBoolean:
			s = "0"
			if v.Bool {
				s = "1"
			}
		}
		if to == tInteger {
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return Value{Type: to, Num: strconv.FormatInt(i, 10)}, nil
			}
		} else if u, err := strconv.ParseUint(s, 10, 64); err == nil {
			return Value{Type: to, Num: strconv.FormatUint(u, 10)}, nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil || f != math.Trunc(f) || math.IsInf(f, 0) {
			return Value{}, fmt.Errorf("%s %s is not a whole number", v.Type, s)
		}
		if to == tInteger && (f < math.MinInt64 || f >= math.MaxInt64) || to == tUnsigned && (f < 0 || f >= math.MaxUint64) {
			return Value{}, fmt.Errorf("%s is out of range for %s", s, to)
		}
		if to == tInteger {
			return Value{Type: to, Num: strconv.FormatInt(int64(f), 10)}, nil
		}
		return Value{Type: to, Num: strconv.FormatUint(uint64(f), 10)}, nil
	case tBoolean:
		switch v.Type {
		case tString:
			b, err := strconv.ParseBool(strings.TrimSpace(v.Str))
			if err != nil {
				return Value{}, fmt.Errorf("string %q is not a boolean", v.Str)
			}
			return Value{Type: to, Bool: b}, nil
		case tFloat, tInteger, tUnsigned:
			switch v.Num {
			case "0":
				return Value{Type: to, Bool: false}, nil
			case "1":
				return Value{Type: to, Bool: true}, nil
			}
			return Value{}, fmt.Errorf("%s %s is not 0 or 1", v.Type, v.Num)
		}
	}
	return Value{}, fmt.Errorf("cannot cast %s to %q", v.Type, to)
}
