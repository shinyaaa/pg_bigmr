# pg_bigmr (WIP)
A pg_bigm clone with Rust.
This is an experimental project to implement pg_bigm using Rust and pgrx.

## Difference with pg_bigm
|                                | pg_bigm         | pg_bigmr       |
| ------------------------------ | --------------- | -------------- |
| Extension name                 | pg_bigm         | pg_bigmr       |
| Implementaion language         | C               | Rust           |
| Placeholder for GUC parameters | pg_bigm.*       | pg_bigmr.*     |
| Supported versions             | PostgreSQL 9.1+ | PostgreSQL 12+ |

## Install
You can install `pg_bigmr` by running the command `cargo pgrx install`.
```
$ cargo pgrx install --pg-config=/path/to/pg_config [--sudo] [--release]
```
- `--pg_config` <PG_CONFIG>: The `pg_config` path (default is first in $PATH)
- `--sudo`: Use `sudo` to install the extension artifacts
- `--release`: Compile for release mode (default is debug)

## How to use
Add `pg_bigmr` to the `shared_preload_libraries` parameter, and run `CREATE EXTENSION pg_bigmr;`.
```
$ echo "shared_preload_libraries = 'pg_bigmr'" >> $PGDATA/postgresql.conf
$ pg_ctl start
$ psql
=# CREATE EXTENSION pg_bigmr;
=# \dx pg_bigmr
                                  List of installed extensions
   Name   | Version | Schema |                           Description                            
----------+---------+--------+------------------------------------------------------------------
 pg_bigmr | 0.1.0   | public | text similarity measurement and index searching based on bigrams
(1 row)
```

## License
pg_bigmr is released under the [PostgreSQL License](https://opensource.org/license/postgresql), a liberal Open Source license, similar to the BSD or MIT licenses.