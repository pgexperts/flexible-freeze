#!/bin/sh

DB=ff
dropdb "$DB"
createdb "$DB"
export PGDATABASE="$DB"

N_ROWS=10000000

p() {
    psql --no-psqlrc --echo-queries --command "$@"
}


p "create table if not exists nine_tenths_bloat as select * from generate_series(1, $N_ROWS) as id"
p "delete from nine_tenths_bloat where id % 10 <> 1"
p "analyze nine_tenths_bloat"


p "create schema if not exists ff_schema"
p "create table if not exists ff_schema.eight_tenths_bloat as select * from generate_series(1, $N_ROWS) as id"
p "delete from ff_schema.eight_tenths_bloat where id % 10 not in (1, 2)"
p "analyze ff_schema.eight_tenths_bloat"


