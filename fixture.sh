#!/bin/sh

DB=ff

dropdb "$DB"
createdb "$DB"
export PGDATABASE="$DB"

N_ROWS=10000000
psql -c "drop table nine_tenths_bloat"
psql -c "create table nine_tenths_bloat as select * from generate_series(1, $N_ROWS) as id"
psql -c "delete from nine_tenths_bloat where id % 10 <> 1"
psql -c "analyze nine_tenths_bloat"
