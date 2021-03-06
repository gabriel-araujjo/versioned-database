Extensible database versioning.

# Drivers

1. [Postgres](https://github.com/gabriel-araujjo/psql-versioning)

# Usage

```go
package my_package

import "database/sql"
import _ "github.com/lib/pq"
import "github.com/gabriel-araujjo/versioned-database"
import "github.com/gabriel-araujjo/psql-versioning"

type scheme struct {}

func (s *scheme) OnCreate(db *sql.DB) error {
	_, err := db.Exec("CREATE TABLE user (id serial PRIMARY KEY, name text, password bytea)")
	return err
}

func (s *scheme) OnUpdate(db *sql.DB, oldVersion int) error {
	_, err := db.Exec("DROP TABLE IF EXISTS user")
	if err != nil {
		return err
	}
	return s.OnCreate(db)
}

func (s *scheme) VersionStrategy() string {
    return "psql-versioning"
}

func (s *scheme) Version() int {
	return 1
}

func main() {
    db, err := sql.Open("postgres", "user=pqgotest dbname=pqgotest sslmode=verify-full")
    if err != nil {
    	log.Fatal(err)
    }
    
    version.PersistScheme(db, new(scheme))
}

```