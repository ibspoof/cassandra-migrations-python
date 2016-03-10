# Schema migration for Cassandra

Python script for generating, executing and rolling back Cassandra CQL [schema migrations](http://en.wikipedia.org/wiki/Schema_migration).

Based on previous script [cassandra-migrations](https://github.com/ibspoof/cassandra-migrations)

## Installation

Download the script, give execution permission and install the python dependencies

```
$ chmod +x cassandra-migrations.py
$ pip install cassandra-driver
$ pip install blist
```

## Usage

### Help

Basic Help
```bash
./cassandra-migrations.py -h
```

Full Help
```bash
./cassandra-migrations.py help full
```

### Create Migration

```
./cassandra-migrations.py generate {keyspace} --name {name}
```
This creates a new file  /migrations/{keyspace}/20140914222010_{name}.json
```json
{
    "up": [
        "// add up cql events one per line"
    ],
    "down": [
        "// add down cql events one per line"
    ]
}
```

### Execute Migration

Apply migrations to a keyspace:
```
./cassandra-migrations.py migrate {keyspace}
```

for a remote server
```
./cassandra-migrations.py migrate {keyspace} --ip {serverIP}
```

for a remote server w/ authentication
```
./cassandra-migrations.py migrate {keyspace} --ip {serverIP} --username {username} --password {password}
```

### Rollback Migration
```
./cassandra-migrations.py rollback {keyspace}
```
for rolling back more than one migration version
```
./cassandra-migrations.py rollback {keyspace} --steps 2
```

## Get Latest Migration Version
```
./cassandra-migrations.py current {keyspace}
```
or
```
./cassandra-migrations.py current {keyspace} --ip {serverIP}
```

### All Options
```bash
--name              Name of schema migration to use when using 'generate' task.
--username          Cassandra server Username (default: None)
--password          Cassandra server Password (default: None)
--ip                Cassandra server IP address (default: 127.0.0.1)
--port              Cassandra server Port (default: 9042)
--con               C* Insert Consistency (default: LOCAL_QUARUM)
--timeout           Session default_timeout (default: 60s)
--debug             Should debug console logs be enabled (default: False)
--protocol_version  CQL protocol_version (default: 3)
--sleep             Sleep time between migrations in seconds (default: 0.1)
--steps             Rollback number of migrations (default: 1)
```

## Changes from original cassandra-migrations
URL: https://github.com/ibspoof/cassandra-migrations
- Moved to JSON as migration file format
- Changed rollback operation to be based on latest DB entries instead of files
- Added `--steps` option for rolling back multiple migrations
- Added `--sleep`, `--protocol_version` and `--debug` options


## Change Log

**[2016-03-09](https://github.com/ibspoof/cassandra-migrations-python/tree/2016-03-09)**
- Initial release
