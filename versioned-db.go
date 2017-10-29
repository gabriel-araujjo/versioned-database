package versioned_database

import (
	"database/sql"
	"sync"
	"fmt"
	"errors"
)

type VersioningDriver interface {
	Version(db *sql.DB) (int, error)
	SetVersion(db *sql.DB, version int) (error)
}

var (
	versionDriversMu sync.RWMutex
	versionDrivers   = make(map[string]VersioningDriver)
)

// Register makes a v versionDriver available for a versioned
// database to use by the provided name
// It panics if the passed versionDriver is nil or if a versionDriver already is
// registered with the same name
func Register(name string, driver VersioningDriver) {
	versionDriversMu.Lock()
	defer versionDriversMu.Unlock()

	if driver == nil {
		panic("versioned db: Register versionDriver is nil")
	}
	if _, dup := versionDrivers[name]; dup {
		panic("versioned db: Register called twice for versionDriver " + name)
	}
	versionDrivers[name] = driver
}

type VersioningStrategy interface {
	OnCreate(db *sql.DB) error
	OnUpdate(db *sql.DB, oldVersion int) error
}

func VersionedDatabase(versioningDriverName string, db *sql.DB, version int, strategy VersioningStrategy) error  {
	var err error

	if db == nil {
		return errors.New("versioned db: db is nil")
	}
	if strategy == nil {
		return errors.New("versioned db: strategy is nil")
	}

	versionDriversMu.RLock()
	versionDriver, ok := versionDrivers[versioningDriverName]
	versionDriversMu.RUnlock()
	if !ok {
		return fmt.Errorf("versioned db: unknown v driver %q (forgotten import?)", versioningDriverName)
	}

	dbVersion, err := versionDriver.Version(db)

	if err == nil {
		if dbVersion == 0 {
			err = strategy.OnCreate(db)
		} else if dbVersion < version {
			err = strategy.OnUpdate(db, dbVersion)
		} else {
			return nil
		}
	}

	if err == nil {
		return versionDriver.SetVersion(db, version)
	}

	return err
}




