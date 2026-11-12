package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestAppendLineEscaping(t *testing.T) {
	p := &Point{
		Measurement: "cpu load,x",
		Tags:        map[string]string{"host": "a b,c=d", "empty": "", "dc": `x\y`},
		Fields: []FieldValue{
			{Key: "f v", Value: Value{Type: tFloat, Num: "1"}},
			{Key: "i", Value: Value{Type: tInteger, Num: "9007199254740993"}},
			{Key: "u", Value: Value{Type: tUnsigned, Num: "18446744073709551615"}},
			{Key: "s", Value: Value{Type: tString, Str: `say "hi" \ bye`}},
			{Key: "b", Value: Value{Type: tBoolean, Bool: true}},
		},
		Time: "1700000000000000001",
	}
	got := string(AppendLine(nil, p))
	want := `cpu\ load\,x,dc=x\\y,host=a\ b\,c\=d f\ v=1,i=9007199254740993i,u=18446744073709551615u,s="say \"hi\" \\ bye",b=true 1700000000000000001` + "\n"
	if got != want {
		t.Fatalf("got  %q\nwant %q", got, want)
	}
}

func TestDecodeKeepsIntegers(t *testing.T) {
	var row []any
	dec := json.NewDecoder(strings.NewReader(`[9007199254740993, 1.5, "x", true]`))
	dec.UseNumber()
	if err := dec.Decode(&row); err != nil {
		t.Fatal(err)
	}
	v, err := decodeValue(row[0], tInteger)
	if err != nil || v.Num != "9007199254740993" {
		t.Fatalf("got %+v, %v", v, err)
	}
	if _, err := decodeValue(row[2], tFloat); err == nil {
		t.Fatal("string accepted as float")
	}
}

func TestCast(t *testing.T) {
	cases := []struct {
		in   Value
		to   string
		want string
		err  bool
	}{
		{Value{Type: tInteger, Num: "7"}, tFloat, "7", false},
		{Value{Type: tFloat, Num: "7"}, tInteger, "7", false},
		{Value{Type: tFloat, Num: "1.5"}, tInteger, "", true},
		{Value{Type: tFloat, Num: "1e3"}, tInteger, "1000", false},
		{Value{Type: tString, Str: " 42 "}, tInteger, "42", false},
		{Value{Type: tString, Str: "abc"}, tFloat, "", true},
		{Value{Type: tString, Str: "2.5"}, tFloat, "2.5", false},
		{Value{Type: tInteger, Num: "-1"}, tUnsigned, "", true},
		{Value{Type: tFloat, Num: "2.5"}, tString, "2.5", false},
		{Value{Type: tBoolean, Bool: true}, tInteger, "1", false},
		{Value{Type: tInteger, Num: "2"}, tBoolean, "", true},
		{Value{Type: tString, Str: "true"}, tBoolean, "true", false},
	}
	for _, c := range cases {
		got, err := Cast(c.in, c.to)
		if c.err {
			if err == nil {
				t.Errorf("Cast(%+v, %s) = %+v, want error", c.in, c.to, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Cast(%+v, %s): %v", c.in, c.to, err)
			continue
		}
		s := got.Num
		if got.Type == tString {
			s = got.Str
		}
		if got.Type == tBoolean {
			s = map[bool]string{true: "true", false: "false"}[got.Bool]
		}
		if s != c.want || got.Type != c.to {
			t.Errorf("Cast(%+v, %s) = %+v, want %s", c.in, c.to, got, c.want)
		}
	}
}

func TestPickValue(t *testing.T) {
	n := func(s string) json.Number { return json.Number(s) }
	// InfluxQL fills every numeric column of a conflicting field.
	v, _ := pickValue(map[string]any{tFloat: n("1.5"), tInteger: n("1")}, []string{tFloat, tInteger}, false)
	if v.Type != tFloat || v.Num != "1.5" {
		t.Fatalf("got %+v", v)
	}
	v, _ = pickValue(map[string]any{tFloat: n("9007199254740992"), tInteger: n("9007199254740993")}, []string{tFloat, tInteger}, false)
	if v.Type != tInteger || v.Num != "9007199254740993" {
		t.Fatalf("got %+v", v)
	}
	v, _ = pickValue(map[string]any{tString: "x"}, []string{tBoolean, tFloat, tString}, false)
	if v.Type != tString || v.Str != "x" {
		t.Fatalf("got %+v", v)
	}
	v, _ = pickValue(map[string]any{tFloat: n("20"), tInteger: n("20")}, []string{tFloat, tInteger}, true)
	if v.Type != tFloat || v.Num != "20" {
		t.Fatalf("backup mode: got %+v", v)
	}
}

func schema() *Schema {
	return &Schema{
		Fields:     []string{"a", "b", "c"},
		FieldTypes: map[string][]string{"a": {tFloat}, "b": {tInteger}, "c": {tFloat, tString}},
		Tags:       []string{"host", "region"},
	}
}

func TestCheck(t *testing.T) {
	r := NewRules()
	if err := r.Check(schema()); err == nil || !strings.Contains(err.Error(), `field "c" has several types`) {
		t.Fatalf("conflict not reported: %v", err)
	}
	r.Cast["c"] = tString
	if err := r.Check(schema()); err != nil {
		t.Fatal(err)
	}
	r.RenameField["a"] = "b"
	if err := r.Check(schema()); err == nil || !strings.Contains(err.Error(), `would both become field "b"`) {
		t.Fatalf("field rename collision not reported: %v", err)
	}
	r.RenameField["b"] = "bb" // b moves away, so a may take its name
	if err := r.Check(schema()); err != nil {
		t.Fatal(err)
	}
	r.RenameTag["host"] = "c"
	if err := r.Check(schema()); err == nil || !strings.Contains(err.Error(), `tag "host" and field "c"`) {
		t.Fatalf("tag/field collision not reported: %v", err)
	}
	delete(r.RenameTag, "host")
	r.DropTag["nope"] = true
	if err := r.Check(schema()); err == nil || !strings.Contains(err.Error(), `no tag "nope"`) {
		t.Fatalf("unknown tag not reported: %v", err)
	}
}

func TestApply(t *testing.T) {
	r := NewRules()
	r.RenameTag["host"] = "hostname"
	r.DropTag["region"] = true
	r.TagValue["host"] = map[string]string{"old": "new"}
	r.RenameField["a"] = "alpha"
	r.DropField["b"] = true
	r.Cast["c"] = tString
	p := &Point{
		Tags: map[string]string{"host": "old", "region": "eu"},
		Fields: []FieldValue{
			{Key: "a", Value: Value{Type: tFloat, Num: "1"}},
			{Key: "b", Value: Value{Type: tInteger, Num: "2"}},
			{Key: "c", Value: Value{Type: tFloat, Num: "3.5"}},
		},
		Time: "10",
	}
	keep, err := r.Apply(p, "m2")
	if err != nil || !keep {
		t.Fatal(keep, err)
	}
	got := string(AppendLine(nil, p))
	if want := "m2,hostname=new alpha=1,c=\"3.5\" 10\n"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	only := &Point{Tags: map[string]string{}, Fields: []FieldValue{{Key: "b", Value: Value{Type: tInteger, Num: "2"}}}, Time: "1"}
	if keep, _ := r.Apply(only, "m2"); keep {
		t.Fatal("point without fields kept")
	}
}

func TestWindows(t *testing.T) {
	w := windows(int64(90*time.Minute), int64(3*time.Hour), time.Hour)
	if len(w) != 3 || w[0][0] != int64(time.Hour) || w[2][1] != int64(4*time.Hour) {
		t.Fatalf("got %v", w)
	}
	w = windows(-5, 5, 10)
	if len(w) != 2 || w[0][0] != -10 || w[1][1] != 10 {
		t.Fatalf("got %v", w)
	}
}

func TestBuildRules(t *testing.T) {
	r, err := buildRules("", nil, nil, []string{"host:a=b=c", "dc:x="}, []string{"x=y"}, nil, []string{"v=float"})
	if err != nil {
		t.Fatal(err)
	}
	if r.TagValue["host"]["a"] != "b=c" || r.TagValue["dc"]["x"] != "" || r.RenameField["x"] != "y" || r.Cast["v"] != tFloat {
		t.Fatalf("got %+v", r)
	}
	if _, err := buildRules("", []string{"novalue"}, nil, nil, nil, nil, nil); err == nil {
		t.Fatal("bad --rename-tag accepted")
	}
}

func TestConvert(t *testing.T) {
	r := NewRules()
	r.TagToField["region"] = true
	r.FieldToTag["b"] = true
	if err := r.Check(schema()); err == nil || !strings.Contains(err.Error(), `field "c" has several types`) {
		t.Fatalf("got %v", err)
	}
	r.Cast["c"] = tFloat
	if err := r.Check(schema()); err != nil {
		t.Fatal(err)
	}
	r.FieldToTag["a"] = true
	r.RenameTag["host"] = "a"
	if err := r.Check(schema()); err == nil || !strings.Contains(err.Error(), `would both become tag "a"`) {
		t.Fatalf("tag collision not reported: %v", err)
	}
	delete(r.RenameTag, "host")
	delete(r.FieldToTag, "a")
	p := &Point{
		Tags:   map[string]string{"host": "h", "region": "eu"},
		Fields: []FieldValue{{Key: "a", Value: Value{Type: tFloat, Num: "1"}}, {Key: "b", Value: Value{Type: tInteger, Num: "42"}}},
		Time:   "5",
	}
	if keep, err := r.Apply(p, "m"); err != nil || !keep {
		t.Fatal(keep, err)
	}
	if got, want := string(AppendLine(nil, p)), "m,b=42,host=h a=1,region=\"eu\" 5\n"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	only := &Point{Tags: map[string]string{}, Fields: []FieldValue{{Key: "b", Value: Value{Type: tInteger, Num: "1"}}}, Time: "7"}
	if _, err := r.Apply(only, "m"); err == nil {
		t.Fatal("point without fields after field-to-tag accepted")
	}
}
