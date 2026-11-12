# influx-reshape

Rename, drop and retype tags, fields and measurements in InfluxDB 1.x and
2.x, online, with a count check after every step.

InfluxQL has no `ALTER`: there is no way to rename a tag, a field or a
measurement, to drop a single field, or to fix a field whose type differs
between shards (`field type conflict`). influx-reshape copies the
measurement through the HTTP API with the changes applied, verifies the
copy, and swaps it in. It runs against a live server; it does not touch TSM
files.

```
influx-reshape plan  --db home -m sensors --rename-tag room=location --drop-field battery --cast temperature=float --scan
influx-reshape apply --db home -m sensors --rename-tag room=location --drop-field battery --cast temperature=float --backup sensors.lp.gz --yes
```

## Why

These are among the oldest open feature requests of InfluxDB:

- [influxdata/influxdb#4157](https://github.com/influxdata/influxdb/issues/4157)
  "RENAME TAG": 271 reactions, 49 comments, open since 09/2015, last
  comment 10/2025.
- [influxdata/influxdb#6150](https://github.com/influxdata/influxdb/issues/6150)
  "Drop field": 204 reactions, 52 comments, open since 03/2016.
- [influxdata/influxdb#4155](https://github.com/influxdata/influxdb/issues/4155)
  "RENAME MEASUREMENT": 119 reactions, 68 comments, open since 09/2015.
  (Counts as of 2026-11-12.)

The workaround in these threads is `SELECT * INTO new FROM old GROUP BY *`,
then dropping the old measurement. Users there report that it turns tags
into fields when `GROUP BY *` is forgotten, and a documentation FAQ linked
in #4155 explains why `INTO` queries miss data. It also loses data silently when a field has a type
conflict: `SELECT *` returns only one of the field's types. On InfluxDB
1.11.8, a measurement with a float, a string and a boolean value of the
same field returned one row for `SELECT *` and `count_v = 1` for
`COUNT(*)`. The end-to-end tests here check this on every run.

The only tool linked in the threads is
[infix](https://github.com/Abc-Arbitrage/infix) (41 stars, last commit
02/2023), which rewrites TSM files offline and needs the server to be
stopped; it works on 1.x only. I found nothing for 2.x.

## How it works

`plan` reads the schema (`SHOW FIELD KEYS`, `SHOW TAG KEYS`), the time
range and the number of values per field, checks the changes and prints
the steps. With `--scan` it also runs every point through the changes
without writing, to find failing casts and collisions.

`apply` runs these steps:

1. optional: `--backup FILE.gz` writes the original points as gzipped line
   protocol. After `gunzip` it can be posted to `/write` as it is (the `#`
   header lines are line protocol comments); the tests restore it this way.
2. copy the measurement to `<name>__reshape_tmp` with the changes applied.
3. drop the original measurement.
4. copy `<name>__reshape_tmp` back to the original name.
5. drop `<name>__reshape_tmp`.

With `--rename-measurement` it copies to the new name and drops the old one.

Details:

- Points are read in time windows (`--window`, default 24h) with
  `GROUP BY *`, so tags stay tags, and with an explicit select list
  `"field"::type` for every type of every field. This is what makes type
  conflicts readable at all.
- After each window it counts the values per field in the target
  (`COUNT(*)` over the window) and compares them with what it wrote. A
  mismatch stops the run before anything is dropped.
- Line protocol is written with nanosecond timestamps. Numbers are passed
  through as decimal text, so integers above 2^53 stay exact, and integers
  keep their `i` suffix (`u` for unsigned; unsigned fields are not covered
  by the tests).
- Progress is saved after every window (`--state`). After an error, the
  same command continues where it stopped. Writes are
  idempotent in InfluxDB (same series and timestamp overwrite), so a
  repeated window does no harm.
- On 1.x, `DROP MEASUREMENT` removes the field keys and types, so after a
  cast `SHOW FIELD KEYS` lists one type again. On 2.x the drop is
  `/api/v2/delete` with `_measurement="..."` on the bucket behind the DBRP
  mapping. The run stops if the measurement still has data after the drop
  (something still writes to it).

Changes (all repeatable, combinable in one run):

| flag | effect |
|---|---|
| `--rename-measurement NEW` | write to `NEW`, drop the old measurement |
| `--rename-tag OLD=NEW` | rename a tag key |
| `--drop-tag KEY` | remove a tag (series merge, see collisions) |
| `--tag-value KEY:OLD=NEW` | change one tag value; `KEY:OLD=` removes it |
| `--rename-field OLD=NEW` | rename a field key |
| `--drop-field KEY` | remove a field; points without fields disappear |
| `--cast FIELD=TYPE` | convert to `float`, `integer`, `unsigned`, `string` or `boolean` |
| `--tag-to-field KEY` | turn a tag into a string field with the same key (series merge) |
| `--field-to-tag KEY` | turn a field into a tag with the same key; the value becomes its text |

Safety checks before anything is written: every rule must name an existing
key; renames must not land on a key that stays; a field with several types
needs `--cast` or `--drop-field`; the staging or target measurement must be
empty; on 1.x the measurement must not have data in another retention
policy, because `DROP MEASUREMENT` deletes it in all retention policies of
the database. Casts are strict: `1.5` to integer, `"abc"` to float or `2`
to boolean stop the run.

A point whose only field is turned into a tag would have no field left;
that stops the run as well (`plan --scan` finds it beforehand).

Collisions: when `--drop-tag`, `--tag-value` or `--tag-to-field` makes two
points share a
series and a timestamp, InfluxDB would keep only the last value of each
field. The copy detects that and stops; `--allow-collisions` accepts it
(the count check then expects the merged numbers).

## Install

Go 1.24 or newer, no dependencies outside the standard library:

```
go install github.com/Jwrede/influx-reshape@latest
```

Connection flags: `--url` (default `http://localhost:8086`), `--db`,
`--rp`, `--user`/`--password` for 1.x, `--token`/`--org` for 2.x. The
environment variables `INFLUX_URL`, `INFLUX_USERNAME`, `INFLUX_PASSWORD`,
`INFLUX_TOKEN` and `INFLUX_ORG` work as well. On 2.x, `--db` is the
database name of a DBRP mapping; every bucket has a virtual mapping with
its own name.

## Example

A `sensors` measurement over two months; a second script wrote
`temperature` as an integer (`20i`) instead of a float from February on,
so the field has a type conflict. Output from InfluxDB 1.11.8:

```
$ influx-reshape plan --db home -m sensors --rename-tag room=location --drop-field battery --cast temperature=float --scan
InfluxDB v1.11.8, database "home", retention policy "autogen"
measurement "sensors": 2026-01-01T00:00:00Z .. 2026-03-01T23:00:00Z
  tags: "room", "sensor_id"
  field "battery": integer (2544)
  field "temperature": float/integer (2544)  <- type conflict; SELECT * returns only one of these types
changes:
  rename tag "room" -> "location"
  drop field "battery"
  cast field "temperature" to float
steps:
  1. copy "sensors" -> "sensors__reshape_tmp" with the changes, verify counts
  2. drop measurement "sensors"
  3. copy "sensors__reshape_tmp" -> "sensors", verify counts
  4. drop measurement "sensors__reshape_tmp"
scan: 2544 points would become 2544 points
  field "temperature": float (2544)

$ influx-reshape apply --db home -m sensors --rename-tag room=location --drop-field battery --cast temperature=float --backup sensors.lp.gz --yes
step 1/5: back up "sensors" to sensors.lp.gz
  2544 points written to sensors.lp.gz
step 2/5: copy "sensors" -> "sensors__reshape_tmp" with the changes, verify counts
  2544 points read, 2544 written, 2544 values verified
step 3/5: drop measurement "sensors"
step 4/5: copy "sensors__reshape_tmp" -> "sensors", verify counts
  2544 points read, 2544 written, 2544 values verified
step 5/5: drop measurement "sensors__reshape_tmp"
done
```

Afterwards `SHOW TAG KEYS FROM sensors` lists `location` and `sensor_id`,
and `SHOW FIELD KEYS FROM sensors` lists only `temperature float`.

## Tests

- `go test ./...`: line protocol escaping, exact integers, casts, reading
  conflicting fields, rule checks, windows, flag parsing.
- `IR_URL=http://127.0.0.1:8086 go test -run E2E -v ./...` runs against a
  real server (for 2.x also `IR_DB`, `IR_TOKEN`, `IR_ORG`). CI runs it
  against InfluxDB 1.8, 1.11, 2.7 and the latest 2.x image (on 2026-11-12:
  1.8.10, 1.11.8, 2.7.12, 2.9.1), and weekly on Mondays. Locally it passed
  on 1.11.8 and 2.7.12. The scenarios: rename
  tag, rename and drop field and change a tag value in place over several
  windows, with escaped keys and values, an empty string, booleans and an
  integer above 2^53, checked point by point and against `SHOW FIELD KEYS`;
  a float/integer/string conflict across shards, where `SELECT *` is shown
  to miss values, a failing strict cast leaves the source unchanged and
  `--cast v=float` keeps all four values; rename measurement with backup and
  a restore of that backup; a backup of a numeric conflict that restores
  without a type conflict; collisions from `--drop-tag` (refused, then
  merged with `--allow-collisions`); resume after a failed window; refusal
  of a non-empty staging measurement, of `apply` without `--yes` and, on
  1.x, of a measurement that also exists in another retention policy;
  tags turned into fields and fields into tags, with a check of
  `SHOW TAG KEYS` and `SHOW FIELD KEYS` afterwards, and a point that would
  lose its last field; 24000 points in 40 series with several chunks and
  write batches.

The integration tests found two errors that unit tests did not show: the
time range came from `SELECT * ... LIMIT 1`, which skipped the last point
when it had the other type of a conflicting field; and the backup wrote a
whole float of a conflicting field as an integer.

## Limits

- Stop writers to the measurement first, or switch them to the new schema.
  Points written to the old measurement between its copy and its drop are
  lost; the run only notices writes that arrive after the drop.
- The measurement is copied twice for in-place changes and needs the space
  for one more copy while it runs. Large measurements take as long as
  reading and writing them twice through the HTTP API; nothing here was
  measured on production-sized data.
- InfluxQL converts between float, integer and unsigned, so for a field with
  a numeric type conflict a whole float cannot be told apart from an
  integer. The copy reads whole numbers as integers before the cast; the
  backup writes all values of such a field as floats (exact up to 2^53).
- Collision detection keeps one entry per point of the current window in
  memory; use a smaller `--window` for very dense measurements with
  `--drop-tag`, `--tag-value` or `--tag-to-field`.
- Only one measurement per run, no `WHERE` filter for partial rewrites, no
  continuous queries or retention policies are changed.
- InfluxDB 3 is not supported (the tool refuses it).
- Once, on 1.11.8, after a write that the server had partly rejected with
  `field type conflict`, `SHOW SERIES` still listed the old series keys
  after the rewrite, although queries returned no data for them. I could
  not reproduce it with clean data.

## License

MIT
