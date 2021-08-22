selihc.com/trek
======

A foot-gun free database access layer for Go.

### General Features

- Cannot forget `rows.Close()` and leak database connections
- Transactions are handled via passed functions, nothing to forget to commit or rollback
- Integrated, concurrency-safe migrator built on `fs.FS`
- Sensible functions for running user code between migrations
- Test helpers for rapidly setting up, tearing down, and resetting databases
- No direct dependencies (only imports the database drivers you use)
- Sensible connection tuning out of the box

#### SQLite Specific Features (in-progress)

- Automatic threadsafe write serialization (SQLite only supports one writer at a time)
- `sqlite.NewMemory` to optionally create a purely in-memory database instance
- Clear explanation of build tags to statically compile a go program using sqlite3 

#### Postgresql Specific Features

- Uses advisory locks for concurrency safe migrations
    - Every instance of a program built on trek can run migrations, but only one will actually run them.

```go
//go:embed schema/*.sql
var schema fs.FS

func TestStuff(t *testing.T) {
    // export POSTGRES_DSN=xyz to use a local, already running, postgres instance.
    db := pgtest.NewDB(t, schema, pgmigrate.WithPattern("schema/"))
    defer db.Shutdown()

    for _, c := range cases {
        // helpfully truncate all tables between test cases
        db.Truncate()
    }
}
```

```go
//go:embed schema/*.sql
var schema fs.FS

func main() {
    db := postgresql.NewWrapper(os.Getenv("POSTGRES_DSN"), log.Printf, schema, pgmigrate.WithPattern("schema/"))

    // use db for fun and profit.
}
```

### LICENSE

Do What The Fuck You Want To Public License (WTFPL), see LICENSE for full details
