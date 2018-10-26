#!/bin/sh

N_ROWS=10000000

p() {
    psql --no-psqlrc --echo-queries --command "$@"
}

DB=ff
createdb "$DB" 2>/dev/null
export PGDATABASE="$DB"

p "create table if not exists nine_tenths_bloat as select * from generate_series(1, $N_ROWS) as id"
p "delete from nine_tenths_bloat where id % 10 <> 1"
p "analyze nine_tenths_bloat"

p "create schema if not exists ff_schema"

p "create table if not exists ff_schema.eight_tenths_bloat as select * from generate_series(1, $N_ROWS) as id"
p "delete from ff_schema.eight_tenths_bloat where id % 10 not in (1, 2)"
p "analyze ff_schema.eight_tenths_bloat"

p "create table if not exists ff_schema.nine_tenths_bloat as select * from generate_series(1, $N_ROWS) as id"
p "delete from ff_schema.nine_tenths_bloat where id % 10 <> 1"
p "analyze ff_schema.nine_tenths_bloat"


DB=ff2
createdb "$DB" 2>/dev/null
export PGDATABASE="$DB"

p "create table if not exists nine_tenths_bloat as select * from generate_series(1, $N_ROWS) as id"
p "delete from nine_tenths_bloat where id % 10 <> 1"
p "analyze nine_tenths_bloat"


DB=ff3
createdb "$DB" 2>/dev/null
export PGDATABASE="$DB"

p "create table if not exists ff3_specific_nine_tenths_bloat as select * from generate_series(1, $N_ROWS) as id"
p "delete from ff3_specific_nine_tenths_bloat where id % 10 <> 1"
p "analyze ff3_specific_nine_tenths_bloat"
